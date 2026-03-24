"""
Crime statistics pipeline — Crime stats per commune.

Schema: crime_stats
Tables: one per year (e.g. crime_stats.y2024)

Sources:
- Crime stats (data.gouv.fr): https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales
- Commune geometries (Etalab cadastre): https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/
"""

import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rich.console import Console

from common import (
    delete_existing_departments,
    download_file,
    ensure_schema,
    load_geodataframe,
)
from settings.db import ensure_postgis

console = Console()

CRIME_CSV_URL = (
    "https://static.data.gouv.fr/resources/"
    "bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-"
    "enregistree-par-la-police-et-la-gendarmerie-nationales/"
    "20250710-144817/"
    "donnee-data.gouv-2024-geographie2025-produit-le2025-06-04.csv.gz"
)

CADASTRE_BASE_URL = (
    "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements"
)

SCHEMA = "crime_stats"


def load_crime_data(csv_path: Path, year: int, departements: list[str]) -> pd.DataFrame:
    """Load and pivot crime data: one row per commune, one column per indicator."""
    df = pd.read_csv(
        csv_path,
        sep=";",
        usecols=["CODGEO_2025", "annee", "indicateur", "taux_pour_mille", "nombre"],
        low_memory=False,
    )

    df = df[df["annee"] == year]

    dep_prefixes = tuple(departements)
    df = df[df["CODGEO_2025"].str.startswith(dep_prefixes)]

    # Parse numeric values (French decimal separator)
    for col in ("taux_pour_mille", "nombre"):
        df[col] = (
            df[col].astype(str).str.replace(",", ".").apply(pd.to_numeric, errors="coerce")
        )

    pivot_rate = df.pivot_table(
        index="CODGEO_2025", columns="indicateur", values="taux_pour_mille", aggfunc="first",
    )
    pivot_count = df.pivot_table(
        index="CODGEO_2025", columns="indicateur", values="nombre", aggfunc="first",
    )

    def clean_col(name: str) -> str:
        return (
            name.lower()
            .replace(" ", "_").replace("'", "")
            .replace("é", "e").replace("è", "e").replace("ê", "e")
            .replace("à", "a").replace("û", "u").replace("ô", "o")
        )

    pivot_rate.columns = [f"taux_{clean_col(c)}" for c in pivot_rate.columns]
    pivot_count.columns = [f"nb_{clean_col(c)}" for c in pivot_count.columns]

    result = pivot_rate.join(pivot_count).reset_index()
    result = result.rename(columns={"CODGEO_2025": "code_commune"})

    console.print(f"  -> {len(result)} communes with crime data")
    return result


def load_communes_geom(departements: list[str], dest: Path) -> gpd.GeoDataFrame:
    """Load commune geometries for given departments."""
    frames = []
    for dep in departements:
        try:
            url = f"{CADASTRE_BASE_URL}/{dep}/cadastre-{dep}-communes.json.gz"
            path = download_file(url, dest, decompress=True, label=f"commune geometries {dep}")
            gdf = gpd.read_file(path)
            gdf = gdf.rename(columns={"id": "code_commune"})
            frames.append(gdf[["code_commune", "geometry"]])
        except Exception as e:
            console.print(f"  [red]Skipping geometry {dep}: {e}[/]")

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True))
    console.print(f"  -> {len(combined)} commune geometries loaded")
    return combined


def run(year: int, departements: list[str]):
    """Main pipeline: download, pivot, join geometries, and load into DB."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    table = f"y{year}"
    qualified = f"{SCHEMA}.{table}"

    with tempfile.TemporaryDirectory(prefix="geo-crime-") as tmpdir:
        tmp = Path(tmpdir)

        csv_path = download_file(CRIME_CSV_URL, tmp, label="crime statistics")

        console.print("\n[bold]Processing crime data...[/bold]")
        crime_df = load_crime_data(csv_path, year, departements)

        if crime_df.empty:
            console.print(f"[red]No crime data found for year {year}.[/red]")
            return

        console.print("\n[bold]Loading commune geometries...[/bold]")
        communes_geom = load_communes_geom(departements, tmp)

        merged = communes_geom.merge(crime_df, on="code_commune", how="inner")
        merged["departement"] = merged["code_commune"].str[:2]
        console.print(f"  -> {len(merged)} communes with data and geometry")

        if merged.empty:
            console.print("[red]No data after join.[/red]")
            return

        final = gpd.GeoDataFrame(merged)
        final = final.set_crs(epsg=4326)

        delete_existing_departments(qualified, departements)

        console.print(f"\n[bold]Loading into {qualified}...[/bold]")
        load_geodataframe(final, table, SCHEMA)

        console.print(f"[green]Done — {len(final)} communes loaded into {qualified}[/green]")
