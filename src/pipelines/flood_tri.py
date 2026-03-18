"""
TRI flood zones pipeline — Territoires à Risques Importants d'inondation.

Schema: flood_risk
Table: tri_zones

Source: Georisques — Directive Inondation rapportage 2020
        https://files.georisques.fr/di_2020/

Downloads TRI shapefiles per department, extracts flood zone polygons
(inondable layers) for three probability scenarios, reprojects to WGS84,
and loads into PostGIS.

Classification:
  flood_type: river_overflow / runoff / marine_submersion
  scenario:   high_probability / medium_probability / low_probability
"""

import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import delete_existing_departments, ensure_schema
from common.download import download_file
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "flood_risk"
TABLE = "tri_zones"

TRI_URL = "https://files.georisques.fr/di_2020/tri_2020_sig_di_{dep}.zip"

FLOOD_TYPE_LABELS = {
    "01": "river_overflow",
    "02": "runoff",
    "03": "marine_submersion",
}

SCENARIO_LABELS = {
    "01For": "high_probability",
    "02Moy": "medium_probability",
    "03Mcc": "medium_probability_climate_change",
    "04Fai": "low_probability",
}


def _download_tri(dep: str, tmp_dir: Path) -> Path | None:
    """Download and extract TRI ZIP for a department. Returns extract dir or None."""
    url = TRI_URL.format(dep=dep)
    zip_name = f"tri_{dep}.zip"
    zip_path = tmp_dir / zip_name

    try:
        download_file(url, tmp_dir, label=f"TRI {dep}")
    except Exception:
        console.print(f"  [yellow]No TRI data for department {dep}[/]")
        return None

    zip_path = tmp_dir / f"tri_2020_sig_di_{dep}.zip"
    if not zip_path.exists():
        return None

    extract_dir = tmp_dir / f"tri_{dep}"
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    return extract_dir


def _find_inondable_shapefiles(extract_dir: Path) -> list[Path]:
    """Find all inondable (flood zone) shapefiles in extracted TRI data."""
    return sorted(extract_dir.rglob("*inondable_*_s_*.shp"))


def _parse_flood_zones(shapefiles: list[Path], dep: str) -> gpd.GeoDataFrame:
    """Read inondable shapefiles, normalize columns, reproject to WGS84."""
    frames = []

    for shp in shapefiles:
        gdf = gpd.read_file(shp)
        if gdf.empty:
            continue

        gdf = gdf.to_crs(epsg=4326)

        gdf["flood_type"] = gdf["typ_inond"].map(FLOOD_TYPE_LABELS).fillna("unknown")
        gdf["scenario"] = gdf["scenario"].map(SCENARIO_LABELS).fillna("unknown")
        gdf["watercourse"] = gdf.get("cours_deau")
        gdf["tri_id"] = gdf["id_tri"]
        gdf["departement"] = dep

        frames.append(
            gdf[["flood_type", "scenario", "watercourse", "tri_id", "departement", "geometry"]]
        )

    if not frames:
        return gpd.GeoDataFrame()

    merged = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    return merged.rename_geometry("geom")


def _load_to_postgis(gdf: gpd.GeoDataFrame):
    """Load GeoDataFrame into PostGIS with spatial index."""
    qualified = f"{SCHEMA}.{TABLE}"

    gdf.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom "
            f"ON {qualified} USING GIST (geom)"
        ))
        conn.commit()


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    qualified = f"{SCHEMA}.{TABLE}"
    delete_existing_departments(qualified, departements)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        for i, dep in enumerate(departements):
            console.print(
                f"\n[bold cyan]Department {dep} ({i + 1}/{len(departements)})[/]"
            )

            extract_dir = _download_tri(dep, tmp_dir)
            if not extract_dir:
                continue

            shapefiles = _find_inondable_shapefiles(extract_dir)
            console.print(f"  {len(shapefiles)} flood zone layers found")

            if not shapefiles:
                continue

            gdf = _parse_flood_zones(shapefiles, dep)
            if gdf.empty:
                console.print(f"  [yellow]No flood zone polygons for {dep}[/]")
                continue

            console.print(f"  {len(gdf):,} polygons")
            _load_to_postgis(gdf)
            console.print(f"  [green]{len(gdf):,} polygons loaded for {dep}[/]")

    console.print(
        f"\n[bold green]Done — TRI flood zones loaded into {qualified}[/]"
    )
