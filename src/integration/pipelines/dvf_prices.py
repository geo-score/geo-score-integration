"""
DVF pipeline — Median prices per cadastral section.

Schema: dvf_prices
Tables: one per year (e.g. dvf_prices.y2023)

Sources:
- DVF open data: https://files.data.gouv.fr/geo-dvf/latest/csv/
- Cadastral sections (Etalab): https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/
"""

import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rich.console import Console

from integration.common import (
    delete_existing_departments,
    download_file,
    ensure_schema,
    load_geodataframe,
)
from integration.db import ensure_postgis

console = Console()

DVF_BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"
CADASTRE_BASE_URL = (
    "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements"
)

SCHEMA = "dvf_prices"


def extract_section_id(id_parcelle: str) -> str | None:
    """Extract section ID from id_parcelle (e.g. '75101000AB0001' -> '75101000AB')."""
    if pd.isna(id_parcelle) or len(str(id_parcelle)) < 10:
        return None
    return str(id_parcelle)[:10]


def aggregate_dvf(dvf_path: Path) -> pd.DataFrame:
    """Aggregate DVF mutations per cadastral section."""
    cols = [
        "id_mutation",
        "nature_mutation",
        "valeur_fonciere",
        "type_local",
        "surface_reelle_bati",
        "id_parcelle",
    ]
    df = pd.read_csv(dvf_path, usecols=cols, low_memory=False)

    df = df[df["nature_mutation"] == "Vente"].dropna(subset=["valeur_fonciere"])
    df["section_id"] = df["id_parcelle"].map(extract_section_id)
    df = df.dropna(subset=["section_id"])

    bati = df[df["surface_reelle_bati"] > 0].copy()
    bati["prix_m2"] = bati["valeur_fonciere"] / bati["surface_reelle_bati"]

    agg = (
        bati.groupby("section_id")
        .agg(
            prix_m2_median=("prix_m2", "median"),
            prix_m2_mean=("prix_m2", "mean"),
            nb_ventes=("id_mutation", "nunique"),
            surface_mediane=("surface_reelle_bati", "median"),
        )
        .reset_index()
    )
    return agg


def load_sections_geom(sections_path: Path) -> gpd.GeoDataFrame:
    """Load cadastral section geometries."""
    gdf = gpd.read_file(sections_path)
    gdf["section_id"] = gdf["commune"] + gdf["prefixe"].fillna("000") + gdf["code"]
    return gdf[["section_id", "geometry"]]


def run(year: int, departements: list[str]):
    """Main pipeline: download, aggregate, and load into DB."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    table = f"y{year}"
    qualified = f"{SCHEMA}.{table}"

    with tempfile.TemporaryDirectory(prefix="geo-dvf-") as tmpdir:
        tmp = Path(tmpdir)
        all_frames = []

        for dep in departements:
            console.print(f"\n[bold]Department {dep}[/bold]")

            dvf_url = f"{DVF_BASE_URL}/{year}/departements/{dep}.csv.gz"
            dvf_path = download_file(dvf_url, tmp, label=f"DVF {dep} {year}")

            sections_url = f"{CADASTRE_BASE_URL}/{dep}/cadastre-{dep}-sections.json.gz"
            sections_path = download_file(sections_url, tmp, decompress=True, label=f"cadastral sections {dep}")

            console.print("  Aggregating DVF prices...")
            dvf_agg = aggregate_dvf(dvf_path)
            console.print(f"  -> {len(dvf_agg)} sections with sales")

            console.print("  Loading geometries...")
            sections_geom = load_sections_geom(sections_path)

            merged = sections_geom.merge(dvf_agg, on="section_id", how="inner")
            merged["departement"] = dep
            console.print(f"  -> {len(merged)} sections with price and geometry")

            all_frames.append(merged)

        if not all_frames:
            console.print("[red]No data to load.[/red]")
            return

        final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True))
        final = final.set_crs(epsg=4326)

        delete_existing_departments(qualified, departements)

        console.print(f"\n[bold]Loading into {qualified}...[/bold]")
        load_geodataframe(final, table, SCHEMA)

        console.print(f"[green]Done — {len(final)} sections loaded into {qualified}[/green]")
