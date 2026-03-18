"""
MNT sun exposure pipeline — Copernicus DEM 90m aspect classification.

Schema: mnt
Table: exposure

Source: Copernicus GLO-90 DEM (AWS Open Data, no auth required)

Computes slope aspect from DEM and classifies sun exposure into polygons:
  - high_exposure : south-facing (135-225°), slope >= 2°
  - low_exposure  : north-facing (315-360° / 0-45°), slope >= 2°
  - moderate      : east/west-facing, slope >= 2°
  - flat          : flat terrain (slope < 2°)

Adjacent cells of the same class are merged into large polygons via
rasterio.features.shapes (vectorisation).
"""

import tempfile
from pathlib import Path

import geopandas as gpd
import httpx
import numpy as np
import rasterio
from rasterio.features import shapes
from rasterio.merge import merge
from rich.console import Console
from shapely.geometry import shape
from sqlalchemy import text

from common import delete_existing_departments, ensure_schema
from common.overpass import DEP_BBOX
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "mnt"
TABLE = "exposure"
MIN_SLOPE_DEG = 2.0

# Class codes for rasterisation
CLS_FLAT = 0
CLS_HIGH = 1
CLS_LOW = 2
CLS_MODERATE = 3
CLS_LABELS = {
    CLS_FLAT: "flat",
    CLS_HIGH: "high_exposure",
    CLS_LOW: "low_exposure",
    CLS_MODERATE: "moderate",
}

# Copernicus DEM GLO-90 on AWS (public, no auth)
COP_BASE = "https://copernicus-dem-90m.s3.eu-central-1.amazonaws.com"


# ---------------------------------------------------------------------------
# Tile download helpers
# ---------------------------------------------------------------------------

def _tile_name(lat: int, lon: int) -> str:
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    return f"Copernicus_DSM_COG_30_{ns}{abs(lat):02d}_00_{ew}{abs(lon):03d}_00_DEM"


def _tile_url(lat: int, lon: int) -> str:
    name = _tile_name(lat, lon)
    return f"{COP_BASE}/{name}/{name}.tif"


def _tiles_for_bbox(bbox: tuple[float, float, float, float]) -> list[tuple[int, int]]:
    south, west, north, east = bbox
    return [
        (lat, lon)
        for lat in range(int(np.floor(south)), int(np.floor(north)) + 1)
        for lon in range(int(np.floor(west)), int(np.floor(east)) + 1)
    ]


def _download_tile(lat: int, lon: int, cache_dir: Path) -> Path | None:
    name = _tile_name(lat, lon)
    dest = cache_dir / f"{name}.tif"
    if dest.exists():
        return dest

    url = _tile_url(lat, lon)
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        return dest
    except httpx.HTTPStatusError:
        return None  # tile over ocean / no data


# ---------------------------------------------------------------------------
# DEM processing
# ---------------------------------------------------------------------------

def _read_dem(tif_paths: list[Path], bbox: tuple[float, float, float, float]):
    """Read and merge DEM tiles for the given bbox."""
    south, west, north, east = bbox
    datasets = [rasterio.open(p) for p in tif_paths]
    try:
        merged, transform = merge(datasets, bounds=(west, south, east, north))
        return merged[0], transform
    finally:
        for ds in datasets:
            ds.close()


def _classify_raster(dem: np.ndarray, transform) -> np.ndarray:
    """Return a uint8 raster of exposure classes from a DEM array."""
    dem_f = dem.astype(np.float32)
    dem_f[(dem < -1000) | (dem > 9000)] = np.nan

    center_lat_idx = dem.shape[0] // 2
    center_lat = transform.f + center_lat_idx * transform.e
    m_per_deg_lat = 111_320.0
    m_per_deg_lon = m_per_deg_lat * np.cos(np.radians(center_lat))

    dx = abs(transform.a) * m_per_deg_lon
    dy = abs(transform.e) * m_per_deg_lat

    dzdx = np.gradient(dem_f, dx, axis=1)
    dzdy = np.gradient(dem_f, dy, axis=0)

    slope = np.degrees(np.arctan(np.sqrt(dzdx**2 + dzdy**2)))
    aspect = np.degrees(np.arctan2(-dzdx, dzdy))
    aspect = (aspect + 360) % 360

    cls = np.full(slope.shape, CLS_FLAT, dtype=np.uint8)
    has_slope = slope >= MIN_SLOPE_DEG

    cls[has_slope & (aspect >= 135) & (aspect <= 225)] = CLS_HIGH
    cls[has_slope & ((aspect >= 315) | (aspect <= 45))] = CLS_LOW
    cls[
        has_slope
        & ~((aspect >= 135) & (aspect <= 225))
        & ~((aspect >= 315) | (aspect <= 45))
    ] = CLS_MODERATE

    # Mark nodata pixels so they are excluded from vectorisation
    cls[np.isnan(slope)] = 255

    return cls


# ---------------------------------------------------------------------------
# Vectorisation & loading
# ---------------------------------------------------------------------------

def _vectorise(cls_raster: np.ndarray, transform, dep: str) -> gpd.GeoDataFrame:
    """Vectorise the classified raster into polygons."""
    mask = cls_raster != 255
    rows = []
    for geom_dict, value in shapes(cls_raster, mask=mask, transform=transform):
        value = int(value)
        if value == 255:
            continue
        rows.append({
            "exposition": CLS_LABELS[value],
            "departement": dep,
            "geom": shape(geom_dict),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


def _process_department(dep: str, cache_dir: Path):
    bbox = DEP_BBOX.get(dep)
    if not bbox:
        console.print(f"  [yellow]No bbox for department {dep}[/]")
        return

    tiles = _tiles_for_bbox(bbox)
    console.print(f"  Downloading {len(tiles)} DEM tiles...")

    tif_paths: list[Path] = []
    for lat, lon in tiles:
        path = _download_tile(lat, lon, cache_dir)
        if path:
            tif_paths.append(path)

    if not tif_paths:
        console.print(f"  [yellow]No DEM data for {dep}[/]")
        return

    dem, transform = _read_dem(tif_paths, bbox)
    console.print(f"  DEM: {dem.shape[1]}x{dem.shape[0]} pixels")

    cls_raster = _classify_raster(dem, transform)
    gdf = _vectorise(cls_raster, transform, dep)

    if gdf.empty:
        console.print(f"  [yellow]No valid data for {dep}[/]")
        return

    console.print(f"  {len(gdf):,} polygons")

    # --- delete old & load ---
    delete_existing_departments(f"{SCHEMA}.{TABLE}", [dep])

    console.print(f"  Loading into {SCHEMA}.{TABLE}...")
    gdf.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom "
            f"ON {SCHEMA}.{TABLE} USING GIST (geom)"
        ))
        conn.commit()

    console.print(f"  [green]{len(gdf):,} polygons loaded for {dep}[/]")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    with tempfile.TemporaryDirectory() as tmp:
        cache = Path(tmp)
        for i, dep in enumerate(departements):
            console.print(
                f"\n[bold cyan]Department {dep} ({i + 1}/{len(departements)})[/]"
            )
            _process_department(dep, cache)

    console.print(f"\n[bold green]Done — MNT exposure loaded into {SCHEMA}.{TABLE}[/]")
