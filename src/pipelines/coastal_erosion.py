"""
Coastal erosion pipeline — Indicateur national de l'érosion côtière.

Schema: coastal
Table: erosion

Source: Cerema / Géolittoral
        Évolution du trait de côte sur 50+ ans.
"""

import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import httpx
from rich.console import Console
from sqlalchemy import text

from common import ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "coastal"
TABLE = "erosion"

# France métropolitaine shapefile (Lambert 93)
EROSION_URL = "https://geolittoral.din.developpement-durable.gouv.fr/telechargement/couches_sig/N_duree_evolution_trait_cote_L_fr_epsg2154_062018_shape.zip"


def run(departements: list[str] | None = None):
    ensure_postgis()
    ensure_schema(SCHEMA)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        # Download
        console.print("  Downloading coastal erosion shapefile...")
        zip_path = tmp_dir / "erosion.zip"
        try:
            with httpx.stream("GET", EROSION_URL, follow_redirects=True, timeout=120) as r:
                r.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            console.print(f"  [red]Download failed: {e}[/]")
            return

        # Extract
        console.print("  Extracting...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir / "erosion")

        # Find shapefile
        shp_files = list((tmp_dir / "erosion").rglob("*.shp"))
        if not shp_files:
            console.print("[red]No shapefile found[/]")
            return

        console.print(f"  Reading {shp_files[0].name}...")
        gdf = gpd.read_file(shp_files[0])
        console.print(f"  -> {len(gdf)} coastline segments, columns: {list(gdf.columns)[:15]}")

        # Reproject to WGS84
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)

        gdf = gdf.rename_geometry("geom")

        # Load
        qualified = f"{SCHEMA}.{TABLE}"
        console.print(f"\n[bold]Loading {len(gdf):,} segments into {qualified}...[/bold]")

        gdf.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)

        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {SCHEMA}.{TABLE} USING GIST (geom)"
            ))
            conn.commit()

        console.print(f"[green]Done — {len(gdf):,} coastal erosion segments loaded into {qualified}[/green]")
