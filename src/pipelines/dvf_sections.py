"""
Cadastral sections pipeline — Reference geometries for DVF prices.

Schema: dvf_prices
Table: sections

Source:
- Cadastral sections (Etalab): https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/
"""

import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import (
    download_file,
    ensure_schema,
    load_geodataframe,
)
from settings.db import engine, ensure_postgis

console = Console()

CADASTRE_BASE_URL = (
    "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements"
)

SCHEMA = "dvf_prices"
TABLE = "sections"


def load_sections_geom(sections_path: Path, departement: str) -> gpd.GeoDataFrame:
    """Load cadastral section geometries for a department."""
    gdf = gpd.read_file(sections_path)
    gdf = gdf.rename(columns={"id": "section_id"})
    gdf["departement"] = departement
    return gdf[["section_id", "commune", "prefixe", "code", "departement", "geometry"]]


def delete_existing_sections(departements: list[str]):
    """Delete sections for given departments (idempotent upsert)."""
    qualified = f"{SCHEMA}.{TABLE}"
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT to_regclass(:t)"), {"t": qualified}
        ).scalar()
        if exists:
            dep_list = ",".join(f"'{d}'" for d in departements)
            conn.execute(text(
                f"DELETE FROM {qualified} WHERE departement IN ({dep_list})"
            ))
            conn.commit()
            console.print(f"  Cleared existing sections for departments {departements}")


def run(departements: list[str]):
    """Main pipeline: download and load cadastral sections."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    with tempfile.TemporaryDirectory(prefix="geo-dvf-sections-") as tmpdir:
        tmp = Path(tmpdir)
        all_frames = []

        for dep in departements:
            console.print(f"\n[bold]Department {dep}[/bold]")
            try:
                url = f"{CADASTRE_BASE_URL}/{dep}/cadastre-{dep}-sections.json.gz"
                path = download_file(url, tmp, decompress=True, label=f"cadastral sections {dep}")

                gdf = load_sections_geom(path, dep)
                console.print(f"  -> {len(gdf)} sections")
                all_frames.append(gdf)
            except Exception as e:
                console.print(f"  [red]Skipping {dep}: {e}[/]")

        if not all_frames:
            console.print("[red]No data to load.[/red]")
            return

        final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True))
        final = final.set_crs(epsg=4326)

        delete_existing_sections(departements)

        console.print(f"\n[bold]Loading into {SCHEMA}.{TABLE}...[/bold]")
        load_geodataframe(final, TABLE, SCHEMA)

        qualified = f"{SCHEMA}.{TABLE}"
        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_section_id ON {qualified} (section_id)"
            ))
            conn.commit()

        console.print(f"[green]Done — {len(final)} sections loaded into {qualified}[/green]")
