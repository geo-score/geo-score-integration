"""
ICU pipeline — Urban Heat Island indicators from CSTB.

Schema: climate
Table: icu

Source: CSTB Sat4BDNB (data.gouv.fr)
        Cartographie nationale des indicateurs liés à l'îlot de chaleur urbain
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

SCHEMA = "climate"
TABLE = "icu"

ICU_URL = "https://static.data.gouv.fr/resources/cartographie-nationale-des-indicateurs-lies-a-lilot-de-chaleur-urbain/20250114-092141/indicateurs-icu.zip"


def _download_icu(tmp: Path) -> Path | None:
    dest = tmp / "icu.zip"
    console.print("  Downloading ICU dataset...")
    try:
        with httpx.stream("GET", ICU_URL, follow_redirects=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        return dest
    except Exception as e:
        console.print(f"  [red]Download failed: {e}[/]")
        return None


def run(departements: list[str] | None = None):
    ensure_postgis()
    ensure_schema(SCHEMA)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        zip_path = _download_icu(tmp_dir)
        if not zip_path:
            return

        console.print("  Extracting...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir / "icu")

        # Find shapefiles or geopackages
        extracted = tmp_dir / "icu"
        geo_files = list(extracted.rglob("*.shp")) + list(extracted.rglob("*.gpkg")) + list(extracted.rglob("*.geojson"))

        if not geo_files:
            console.print("  [red]No spatial files found in archive[/]")
            console.print(f"  Contents: {list(extracted.rglob('*'))[:20]}")
            return

        all_frames = []
        for gf in geo_files:
            try:
                gdf = gpd.read_file(gf)
                console.print(f"  -> {gf.name}: {len(gdf)} features, cols: {list(gdf.columns)[:10]}")

                # Reproject to WGS84 if needed
                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)

                gdf = gdf.rename_geometry("geom")
                all_frames.append(gdf)
            except Exception as e:
                console.print(f"  [yellow]Could not read {gf.name}: {e}[/]")

        if not all_frames:
            console.print("[red]No valid spatial data found.[/red]")
            return

        final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True)).set_crs(epsg=4326)

        qualified = f"{SCHEMA}.{TABLE}"
        console.print(f"\n[bold]Loading {len(final):,} features into {qualified}...[/bold]")

        final.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)

        with engine.connect() as conn:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {SCHEMA}.{TABLE} USING GIST (geom)"))
            conn.commit()

        console.print(f"[green]Done — ICU data loaded into {qualified}[/green]")


# Need pandas for concat
import pandas as pd
