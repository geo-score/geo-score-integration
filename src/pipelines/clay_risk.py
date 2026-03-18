"""
RGA clay shrink-swell risk pipeline — Retrait-gonflement des argiles.

Schema: clay_risk
Table: rga_zones

Source: Georisques / BRGM — Exposition au retrait-gonflement des argiles (2020)
        https://files.georisques.fr/argiles/AleaRG_Fxx_L93.zip

Downloads the national shapefile (122K polygons, Lambert 93), filters by
department, reprojects to WGS84, and loads into PostGIS.

Classification (exposure_level):
  - high   : strong exposure (NIVEAU=3, Fort)
  - medium : medium exposure (NIVEAU=2, Moyen)
  - low    : low exposure (NIVEAU=1, Faible)

Areas not covered by any polygon have residual (negligible) exposure.
"""

import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
from rich.console import Console
from sqlalchemy import text

from common import delete_existing_departments, ensure_schema
from common.download import download_file
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "clay_risk"
TABLE = "rga_zones"

RGA_URL = "https://files.georisques.fr/argiles/AleaRG_Fxx_L93.zip"

EXPOSURE_LABELS = {
    1: "low",
    2: "medium",
    3: "high",
}


def _download_and_extract(tmp_dir: Path) -> Path | None:
    """Download national RGA ZIP and extract shapefile. Returns .shp path."""
    console.print("  Downloading national RGA dataset (~594 MB)...")
    download_file(RGA_URL, tmp_dir, label="RGA national")

    zip_path = tmp_dir / "AleaRG_Fxx_L93.zip"
    if not zip_path.exists():
        console.print("  [red]Download failed[/]")
        return None

    extract_dir = tmp_dir / "rga"
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        console.print("  [red]No shapefile found in archive[/]")
        return None

    return shp_files[0]


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    qualified = f"{SCHEMA}.{TABLE}"
    delete_existing_departments(qualified, departements)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        shp_path = _download_and_extract(tmp_dir)
        if not shp_path:
            return

        console.print("  Reading shapefile...")
        gdf = gpd.read_file(shp_path)
        console.print(f"  {len(gdf):,} total polygons in national dataset")

        # Filter to requested departments
        # DPT column may have leading zeros stripped — normalize both sides
        dep_set = set(departements)
        gdf = gdf[gdf["DPT"].astype(str).str.zfill(2).isin(dep_set)]
        console.print(f"  {len(gdf):,} polygons for departments {departements}")

        if gdf.empty:
            console.print("  [yellow]No RGA data for these departments[/]")
            return

        # Reproject Lambert 93 → WGS84
        gdf = gdf.to_crs(epsg=4326)

        # Normalize columns
        gdf["exposure_level"] = gdf["NIVEAU"].map(EXPOSURE_LABELS).fillna("unknown")
        gdf["departement"] = gdf["DPT"].astype(str).str.zfill(2)

        gdf = gdf[["exposure_level", "departement", "geometry"]]
        gdf = gdf.rename_geometry("geom")

        # Load
        console.print(f"  Loading into {qualified}...")
        gdf.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom "
                f"ON {qualified} USING GIST (geom)"
            ))
            conn.commit()

        console.print(f"  [green]{len(gdf):,} polygons loaded[/]")

    console.print(f"\n[bold green]Done — RGA zones loaded into {qualified}[/]")
