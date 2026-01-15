use crate::error::Result;
use crate::gpkg::gpkg_geometry_to_wkb;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::{Type, ValueRef};
use rusqlite::{Connection, Error};
use wkb::reader::Wkb;

#[derive(Clone, Copy)]
struct Bounds {
    minx: f64,
    maxx: f64,
    miny: f64,
    maxy: f64,
}

/// Register all spatial SQL helper functions in the provided connection.
///
/// Example:
/// ```no_run
/// use rusqlite::Connection;
/// use rusqlite_gpkg::register_spatial_functions;
///
/// let conn = Connection::open_in_memory()?;
/// register_spatial_functions(&conn)?;
/// # Ok::<(), rusqlite_gpkg::GpkgError>(())
/// ```
pub fn register_spatial_functions(conn: &Connection) -> Result<()> {
    register_st_minx(conn)?;
    register_st_miny(conn)?;
    register_st_maxx(conn)?;
    register_st_maxy(conn)?;
    register_st_isempty(conn)?;
    Ok(())
}

pub(crate) fn register_st_minx(conn: &Connection) -> Result<()> {
    register_bounds_component(conn, "ST_MinX", |b| b.minx)
}

pub(crate) fn register_st_miny(conn: &Connection) -> Result<()> {
    register_bounds_component(conn, "ST_MinY", |b| b.miny)
}

pub(crate) fn register_st_maxx(conn: &Connection) -> Result<()> {
    register_bounds_component(conn, "ST_MaxX", |b| b.maxx)
}

pub(crate) fn register_st_maxy(conn: &Connection) -> Result<()> {
    register_bounds_component(conn, "ST_MaxY", |b| b.maxy)
}

pub(crate) fn register_st_isempty(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "ST_IsEmpty",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let wkb = match wkb_from_ctx(ctx)? {
                Some(wkb) => wkb,
                None => return Ok(None),
            };
            let is_empty = bounds_from_geometry(&wkb).is_none();
            Ok(Some(i64::from(is_empty)))
        },
    )?;
    Ok(())
}

fn register_bounds_component<F>(conn: &Connection, name: &str, f: F) -> Result<()>
where
    F: Fn(Bounds) -> f64 + Copy + Send + Sync + 'static,
{
    conn.create_scalar_function(name, 1, FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
        let wkb = match wkb_from_ctx(ctx)? {
            Some(wkb) => wkb,
            None => return Ok(None),
        };
        Ok(bounds_from_geometry(&wkb).map(f))
    })?;
    Ok(())
}

fn wkb_from_ctx<'a>(ctx: &'a Context<'a>) -> std::result::Result<Option<Wkb<'a>>, Error> {
    let value = ctx.get_raw(0);
    match value {
        ValueRef::Null => Ok(None),
        ValueRef::Blob(blob) => {
            let wkb = gpkg_geometry_to_wkb(blob)
                .map_err(|err| Error::UserFunctionError(Box::new(err)))?;
            Ok(Some(wkb))
        }
        _ => Err(Error::InvalidFunctionParameterType(0, Type::Blob)),
    }
}

fn bounds_from_geometry<G: GeometryTrait<T = f64>>(geom: &G) -> Option<Bounds> {
    use geo_traits::GeometryType as GeoType;

    let mut bounds: Option<Bounds> = None;
    match geom.as_type() {
        GeoType::Point(point) => {
            if let Some(coord) = point.coord() {
                add_coord(&mut bounds, &coord);
            }
        }
        GeoType::LineString(line) => {
            for coord in line.coords() {
                add_coord(&mut bounds, &coord);
            }
        }
        GeoType::Polygon(poly) => {
            if let Some(ring) = poly.exterior() {
                add_line_string(&mut bounds, &ring);
            }
            for ring in poly.interiors() {
                add_line_string(&mut bounds, &ring);
            }
        }
        GeoType::MultiPoint(multi) => {
            for point in multi.points() {
                if let Some(coord) = point.coord() {
                    add_coord(&mut bounds, &coord);
                }
            }
        }
        GeoType::MultiLineString(multi) => {
            for line in multi.line_strings() {
                add_line_string(&mut bounds, &line);
            }
        }
        GeoType::MultiPolygon(multi) => {
            for poly in multi.polygons() {
                if let Some(ring) = poly.exterior() {
                    add_line_string(&mut bounds, &ring);
                }
                for ring in poly.interiors() {
                    add_line_string(&mut bounds, &ring);
                }
            }
        }
        GeoType::GeometryCollection(collection) => {
            for sub_geom in collection.geometries() {
                if let Some(sub_bounds) = bounds_from_geometry(&sub_geom) {
                    merge_bounds(&mut bounds, sub_bounds);
                }
            }
        }
        GeoType::Rect(_) | GeoType::Triangle(_) | GeoType::Line(_) => {
            // No GeoPackage geometry types should reach here.
            unreachable!()
        }
    }

    bounds
}

fn add_line_string<L: LineStringTrait<T = f64>>(bounds: &mut Option<Bounds>, line: &L) {
    for coord in line.coords() {
        add_coord(bounds, &coord);
    }
}

fn add_coord<C: CoordTrait<T = f64>>(bounds: &mut Option<Bounds>, coord: &C) {
    let (x, y) = coord.x_y();
    match bounds {
        Some(existing) => {
            existing.minx = existing.minx.min(x);
            existing.maxx = existing.maxx.max(x);
            existing.miny = existing.miny.min(y);
            existing.maxy = existing.maxy.max(y);
        }
        None => {
            *bounds = Some(Bounds {
                minx: x,
                maxx: x,
                miny: y,
                maxy: y,
            });
        }
    }
}

fn merge_bounds(bounds: &mut Option<Bounds>, other: Bounds) {
    match bounds {
        Some(existing) => {
            existing.minx = existing.minx.min(other.minx);
            existing.maxx = existing.maxx.max(other.maxx);
            existing.miny = existing.miny.min(other.miny);
            existing.maxy = existing.maxy.max(other.maxy);
        }
        None => *bounds = Some(other),
    }
}

#[cfg(test)]
mod tests {
    use super::register_spatial_functions;
    use crate::gpkg::wkb_to_gpkg_geometry;
    use geo_types::{Geometry, GeometryCollection, MultiLineString, MultiPoint};
    use geo_types::{LineString, Point};
    use rusqlite::{Connection, params};
    use wkb::reader::Wkb;

    fn gpkg_blob_from_geometry<G: geo_traits::GeometryTrait<T = f64>>(
        geometry: G,
    ) -> crate::Result<Vec<u8>> {
        let mut wkb = Vec::new();
        wkb::writer::write_geometry(&mut wkb, &geometry, &Default::default())?;
        let wkb = Wkb::try_new(&wkb)?;
        wkb_to_gpkg_geometry(wkb, 4326)
    }

    #[test]
    fn st_bounds_for_point() -> crate::Result<()> {
        let conn = Connection::open_in_memory()?;
        register_spatial_functions(&conn)?;

        let point = Point::new(1.5, -2.0);
        let blob = gpkg_blob_from_geometry(point)?;

        let (minx, maxx, miny, maxy, empty): (f64, f64, f64, f64, i64) = conn.query_row(
            "SELECT ST_MinX(?1), ST_MaxX(?1), ST_MinY(?1), ST_MaxY(?1), ST_IsEmpty(?1)",
            params![blob],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;

        assert_eq!(minx, 1.5);
        assert_eq!(maxx, 1.5);
        assert_eq!(miny, -2.0);
        assert_eq!(maxy, -2.0);
        assert_eq!(empty, 0);
        Ok(())
    }

    #[test]
    fn st_is_empty_for_empty_linestring() -> crate::Result<()> {
        let conn = Connection::open_in_memory()?;
        register_spatial_functions(&conn)?;

        let line: LineString<f64> = LineString::new(Vec::new());
        let blob = gpkg_blob_from_geometry(line)?;

        let (minx, empty): (Option<f64>, i64) =
            conn.query_row("SELECT ST_MinX(?1), ST_IsEmpty(?1)", params![blob], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;

        assert!(minx.is_none());
        assert_eq!(empty, 1);
        Ok(())
    }

    #[test]
    fn st_bounds_for_multipoint() -> crate::Result<()> {
        let conn = Connection::open_in_memory()?;
        register_spatial_functions(&conn)?;

        let mp = MultiPoint::from(vec![Point::new(1.0, 5.0), Point::new(-2.0, 3.0)]);
        let blob = gpkg_blob_from_geometry(mp)?;

        let (minx, maxx, miny, maxy): (f64, f64, f64, f64) = conn.query_row(
            "SELECT ST_MinX(?1), ST_MaxX(?1), ST_MinY(?1), ST_MaxY(?1)",
            params![blob],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

        assert_eq!(minx, -2.0);
        assert_eq!(maxx, 1.0);
        assert_eq!(miny, 3.0);
        assert_eq!(maxy, 5.0);
        Ok(())
    }

    #[test]
    fn st_bounds_for_multilinestring() -> crate::Result<()> {
        let conn = Connection::open_in_memory()?;
        register_spatial_functions(&conn)?;

        let line_a = LineString::from(vec![(0.0, 0.0), (2.0, 1.0)]);
        let line_b = LineString::from(vec![(-3.0, 4.0), (-1.0, 2.0)]);
        let mls = MultiLineString(vec![line_a, line_b]);
        let blob = gpkg_blob_from_geometry(mls)?;

        let (minx, maxx, miny, maxy): (f64, f64, f64, f64) = conn.query_row(
            "SELECT ST_MinX(?1), ST_MaxX(?1), ST_MinY(?1), ST_MaxY(?1)",
            params![blob],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

        assert_eq!(minx, -3.0);
        assert_eq!(maxx, 2.0);
        assert_eq!(miny, 0.0);
        assert_eq!(maxy, 4.0);
        Ok(())
    }

    #[test]
    fn st_bounds_for_geometry_collection() -> crate::Result<()> {
        let conn = Connection::open_in_memory()?;
        register_spatial_functions(&conn)?;

        let point = Geometry::Point(Point::new(5.0, -1.0));
        let line = Geometry::LineString(LineString::from(vec![(-2.0, 2.0), (1.0, 3.0)]));
        let collection = GeometryCollection::from(vec![point, line]);
        let blob = gpkg_blob_from_geometry(collection)?;

        let (minx, maxx, miny, maxy): (f64, f64, f64, f64) = conn.query_row(
            "SELECT ST_MinX(?1), ST_MaxX(?1), ST_MinY(?1), ST_MaxY(?1)",
            params![blob],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

        assert_eq!(minx, -2.0);
        assert_eq!(maxx, 5.0);
        assert_eq!(miny, -1.0);
        assert_eq!(maxy, 3.0);
        Ok(())
    }
}
