use std::error::Error;
use std::fmt;

/// Crate error type for GeoPackage operations.
#[derive(Debug)]
pub enum GpkgError {
    /// Wraps errors returned by `rusqlite`.
    Sql(rusqlite::Error),
    /// Wraps errors returned by the `wkb` crate.
    Wkb(wkb::error::WkbError),
    /// Wraps errors returned by Arrow APIs.
    #[cfg(feature = "arrow")]
    Arrow(arrow_schema::ArrowError),
    /// Wraps errors returned by GeoArrow APIs as strings.
    #[cfg(feature = "arrow")]
    GeoArrow(String),
    /// A geometry type in metadata could not be mapped to a supported WKB geometry type.
    UnsupportedGeometryType(String),
    /// A column type declared in SQLite metadata is not supported by this crate.
    UnsupportedColumnType {
        column: String,
        declared_type: String,
    },
    /// Invalid or mixed `z` / `m` dimension flags in GeoPackage metadata.
    InvalidDimension {
        z: i8,
        m: i8,
    },
    /// Property count did not match the layer schema.
    InvalidPropertyCount {
        expected: usize,
        got: usize,
    },
    /// Invalid GeoPackage geometry flags byte.
    InvalidGpkgGeometryFlags(u8),
    /// GeoPackage geometry blob is too short for the fixed header.
    InvalidGpkgGeometryLength {
        len: usize,
        minimum: usize,
    },
    /// GeoPackage geometry blob is too short for the declared envelope payload.
    InvalidGpkgGeometryEnvelope {
        len: usize,
        required: usize,
    },
    /// Dynamic `Value` type did not match the expected conversion target.
    ValueTypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    /// Numeric conversion failed because the value is out of range.
    ValueOutOfRange {
        target: &'static str,
    },
    /// Requested feature property does not exist in the feature.
    MissingProperty {
        property: String,
    },
    /// A layer with the same name already exists.
    LayerAlreadyExists {
        layer_name: String,
    },
    /// Referenced `srs_id` does not exist in `gpkg_spatial_ref_sys`.
    MissingSpatialRefSysId {
        srs_id: u32,
    },
    /// Layer schema has multiple primary key columns, which is unsupported.
    CompositePrimaryKeyUnsupported {
        layer_name: String,
    },
    /// Layer schema has no primary key column.
    MissingPrimaryKeyColumn {
        layer_name: String,
    },
    /// Layer schema has no geometry column.
    MissingGeometryColumn {
        layer_name: String,
    },
    /// A feature row has a `NULL` geometry value.
    NullGeometryValue,
    /// Hybrid/custom VFS registration or usage failed.
    Vfs(String),
    /// Arrow reader observed a value type that did not match the expected Arrow builder type.
    #[cfg(feature = "arrow")]
    InvalidArrowValue {
        expected: &'static str,
        actual: &'static str,
    },
    ReadOnly,
}

impl fmt::Display for GpkgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(err) => write!(f, "{err}"),
            Self::Wkb(err) => write!(f, "{err}"),
            #[cfg(feature = "arrow")]
            Self::Arrow(err) => write!(f, "{err}"),
            #[cfg(feature = "arrow")]
            Self::GeoArrow(err) => write!(f, "{err}"),
            Self::UnsupportedGeometryType(ty) => write!(f, "unsupported geometry type: {ty}"),
            Self::UnsupportedColumnType {
                column,
                declared_type,
            } => write!(
                f,
                "unsupported column type for column '{column}': {declared_type}"
            ),
            Self::InvalidDimension { z, m } => {
                write!(f, "invalid or mixed geometry dimension (z={z}, m={m})")
            }
            Self::InvalidPropertyCount { expected, got } => {
                write!(f, "invalid property count: expected {expected}, got {got}")
            }
            Self::InvalidGpkgGeometryFlags(flags) => {
                write!(f, "invalid gpkg geometry flags: {flags:#04x}")
            }
            Self::InvalidGpkgGeometryLength { len, minimum } => {
                write!(
                    f,
                    "invalid gpkg geometry length: got {len} bytes, expected at least {minimum}"
                )
            }
            Self::InvalidGpkgGeometryEnvelope { len, required } => {
                write!(
                    f,
                    "invalid gpkg geometry envelope length: got {len} bytes, required {required}"
                )
            }
            Self::ValueTypeMismatch { expected, actual } => {
                write!(f, "expected {expected}, got {actual}")
            }
            Self::ValueOutOfRange { target } => {
                write!(f, "value out of range for {target}")
            }
            Self::MissingProperty { property } => write!(f, "missing property: {property}"),
            Self::LayerAlreadyExists { layer_name } => {
                write!(f, "layer already exists: {layer_name}")
            }
            Self::MissingSpatialRefSysId { srs_id } => {
                write!(f, "srs_id {srs_id} not found in gpkg_spatial_ref_sys")
            }
            Self::CompositePrimaryKeyUnsupported { layer_name } => write!(
                f,
                "composite primary keys are not supported yet for layer: {layer_name}"
            ),
            Self::MissingPrimaryKeyColumn { layer_name } => {
                write!(f, "no primary key column found for layer: {layer_name}")
            }
            Self::MissingGeometryColumn { layer_name } => {
                write!(f, "no geometry column found for layer: {layer_name}")
            }
            Self::NullGeometryValue => write!(f, "feature has null geometry value"),
            Self::Vfs(err) => write!(f, "vfs error: {err}"),
            #[cfg(feature = "arrow")]
            Self::InvalidArrowValue { expected, actual } => {
                write!(
                    f,
                    "invalid value for Arrow conversion: expected {expected}, got {actual}"
                )
            }
            Self::ReadOnly => write!(f, "operation not allowed on read-only connection"),
        }
    }
}

impl Error for GpkgError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sql(err) => Some(err),
            Self::Wkb(err) => Some(err),
            #[cfg(feature = "arrow")]
            Self::Arrow(err) => Some(err),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for GpkgError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Sql(err)
    }
}

impl From<wkb::error::WkbError> for GpkgError {
    fn from(err: wkb::error::WkbError) -> Self {
        Self::Wkb(err)
    }
}

pub type Result<T> = std::result::Result<T, GpkgError>;

#[cfg(feature = "arrow")]
impl From<GpkgError> for arrow_schema::ArrowError {
    fn from(value: GpkgError) -> Self {
        arrow_schema::ArrowError::ExternalError(value.into())
    }
}

#[cfg(feature = "arrow")]
impl From<arrow_schema::ArrowError> for GpkgError {
    fn from(value: arrow_schema::ArrowError) -> Self {
        GpkgError::Arrow(value)
    }
}
