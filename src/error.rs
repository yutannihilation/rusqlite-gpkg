use std::error::Error;
use std::fmt;

/// Crate error type for GeoPackage operations.
#[derive(Debug)]
pub enum GpkgError {
    Sql(rusqlite::Error),
    Wkb(wkb::error::WkbError),
    UnsupportedGeometryType(String),
    InvalidDimension { z: i8, m: i8 },
    InvalidPropertyCount { expected: usize, got: usize },
    InvalidGpkgGeometryFlags(u8),
    ReadOnly,
    Message(String),
}

impl fmt::Display for GpkgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(err) => write!(f, "{err}"),
            Self::Wkb(err) => write!(f, "{err}"),
            Self::UnsupportedGeometryType(ty) => write!(f, "unsupported geometry type: {ty}"),
            Self::InvalidDimension { z, m } => {
                write!(f, "invalid or mixed geometry dimension (z={z}, m={m})")
            }
            Self::InvalidPropertyCount { expected, got } => {
                write!(f, "invalid property count: expected {expected}, got {got}")
            }
            Self::InvalidGpkgGeometryFlags(flags) => {
                write!(f, "invalid gpkg geometry flags: {flags:#04x}")
            }
            Self::ReadOnly => write!(f, "operation not allowed on read-only connection"),
            Self::Message(message) => write!(f, "{message}"),
        }
    }
}

impl Error for GpkgError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sql(err) => Some(err),
            Self::Wkb(err) => Some(err),
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

impl From<String> for GpkgError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}

impl From<&str> for GpkgError {
    fn from(message: &str) -> Self {
        Self::Message(message.to_string())
    }
}

pub type Result<T> = std::result::Result<T, GpkgError>;
