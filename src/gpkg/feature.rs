use crate::Value;
use crate::error::{GpkgError, Result};
use rusqlite::types::Type;
use std::collections::HashMap;
use std::sync::Arc;
use wkb::reader::Wkb;

/// A single feature with geometry bytes and owned properties.
pub struct GpkgFeature {
    pub(super) id: i64,
    pub(super) geometry: Option<Vec<u8>>,
    pub(super) properties: Vec<Value>,
    pub(super) property_index_by_name: Arc<HashMap<String, usize>>,
}

impl GpkgFeature {
    /// Return the primary key value.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    /// let layer = gpkg.get_layer("points")?;
    /// let feature = layer.features()?.next().expect("feature");
    /// let _id = feature.id();
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Decode the geometry column into WKB.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    /// let layer = gpkg.get_layer("points")?;
    /// let feature = layer.features()?.next().expect("feature");
    /// let _geom = feature.geometry()?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn geometry(&self) -> Result<Wkb<'_>> {
        let bytes = self.geometry.as_ref().ok_or_else(|| {
            GpkgError::Sql(rusqlite::Error::InvalidColumnType(
                0,
                "geometry".to_string(),
                Type::Null,
            ))
        })?;
        gpkg_geometry_to_wkb(bytes)
    }

    /// Read a property by name as an owned `Value`.
    ///
    /// Example:
    /// ```no_run
    /// use rusqlite_gpkg::Gpkg;
    ///
    /// let gpkg = Gpkg::open_read_only("data/example.gpkg")?;
    /// let layer = gpkg.get_layer("points")?;
    /// let feature = layer.features()?.next().expect("feature");
    /// let value: String = feature
    ///     .property("name")
    ///     .ok_or("missing name")?
    ///     .try_into()?;
    /// # Ok::<(), rusqlite_gpkg::GpkgError>(())
    /// ```
    pub fn property(&self, name: &str) -> Option<Value> {
        match self.property_index_by_name.get(name) {
            Some(idx) => self.properties.get(*idx).cloned(),
            None => None,
        }
    }

    /// Return the ordered property values as stored in the feature.
    pub fn properties(&self) -> &[Value] {
        &self.properties
    }

    #[cfg(test)]
    fn new<G, I>(id: i64, geometry: G, properties: I, property_names: &[&str]) -> Result<Self>
    where
        G: geo_traits::GeometryTrait<T = f64>,
        I: IntoIterator<Item = Value>,
    {
        let mut buf = Vec::new();
        wkb::writer::write_geometry(&mut buf, &geometry, &Default::default())?;
        let mut property_index_by_name = HashMap::with_capacity(property_names.len());
        for (idx, name) in property_names.iter().enumerate() {
            property_index_by_name.insert((*name).to_string(), idx);
        }
        Ok(Self {
            id,
            geometry: Some(buf),
            properties: properties.into_iter().collect(),
            property_index_by_name: Arc::new(property_index_by_name),
        })
    }
}

/// Owned iterator over features.
pub struct GpkgFeatureIterator {
    pub(super) features: std::vec::IntoIter<GpkgFeature>,
}

impl Iterator for GpkgFeatureIterator {
    type Item = GpkgFeature;

    fn next(&mut self) -> Option<Self::Item> {
        self.features.next()
    }
}

/// Strip GeoPackage header and envelope bytes to access raw WKB.
// cf. https://www.geopackage.org/spec140/index.html#gpb_format
pub(crate) fn gpkg_geometry_to_wkb<'a>(b: &'a [u8]) -> Result<Wkb<'a>> {
    let flags = b[3];
    let envelope_size: usize = match flags & 0b00001110 {
        0b00000000 => 0,  // no envelope
        0b00000010 => 32, // envelope is [minx, maxx, miny, maxy], 32 bytes
        0b00000100 => 48, // envelope is [minx, maxx, miny, maxy, minz, maxz], 48 bytes
        0b00000110 => 48, // envelope is [minx, maxx, miny, maxy, minm, maxm], 48 bytes
        0b00001000 => 64, // envelope is [minx, maxx, miny, maxy, minz, maxz, minm, maxm], 64 bytes
        _ => {
            return Err(GpkgError::InvalidGpkgGeometryFlags(flags));
        }
    };
    let offset = 8 + envelope_size;

    Ok(Wkb::try_new(&b[offset..])?)
}

// cf. https://www.geopackage.org/spec140/index.html#gpb_format
pub(crate) fn wkb_to_gpkg_geometry<'a>(wkb: Wkb<'a>, srs_id: u32) -> Result<Vec<u8>> {
    let mut geom = Vec::with_capacity(wkb.buf().len() + 8);
    geom.extend_from_slice(&[
        0x47u8, // magic
        0x50u8, // magic
        0x00u8, // version
        0x01u8, // flags (little endian SRS ID, no envelope)
    ]);
    geom.extend_from_slice(&srs_id.to_le_bytes());
    geom.extend_from_slice(wkb.buf());

    Ok(geom)
}

#[cfg(test)]
mod tests {
    use super::{gpkg_geometry_to_wkb, wkb_to_gpkg_geometry};
    use crate::Result;
    use crate::Value;
    use geo_types::Point;
    use wkb::reader::Wkb;

    #[test]
    fn gpkg_geometry_roundtrip() -> Result<()> {
        let point = Point::new(3.0, -1.0);
        let mut buf = Vec::new();
        wkb::writer::write_geometry(&mut buf, &point, &Default::default())?;
        let wkb = Wkb::try_new(&buf)?;
        let expected = wkb.buf().to_vec();
        let gpkg_blob = wkb_to_gpkg_geometry(wkb, 4326)?;

        let recovered = gpkg_geometry_to_wkb(&gpkg_blob)?;
        assert_eq!(recovered.buf(), expected.as_slice());
        Ok(())
    }

    #[test]
    fn gpkg_geometry_rejects_invalid_flags() {
        let mut blob = vec![0x47, 0x50, 0x00, 0x0A, 0, 0, 0, 0];
        blob.extend_from_slice(&[0; 16]);
        let result = gpkg_geometry_to_wkb(&blob);
        assert!(matches!(
            result,
            Err(crate::error::GpkgError::InvalidGpkgGeometryFlags(_))
        ));
    }

    #[test]
    fn property_invalid_index_reports_error() -> Result<()> {
        let feature =
            super::GpkgFeature::new(1, Point::new(0.0, 0.0), vec![Value::Integer(1)], &["value"])?;
        let value = feature.property("missing");
        assert!(value.is_none());
        Ok(())
    }
}
