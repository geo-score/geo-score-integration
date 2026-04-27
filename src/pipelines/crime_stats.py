"""
Crime statistics pipeline — Crime stats per commune.

Schema: crime_stats
Tables: one per year (e.g. crime_stats.y2024)

Geometries live in `geom_utils.communes` (loaded by `commune-geoms`).
Yearly tables only store crime indicators and join on `code_commune`.

Source:
- Crime stats (data.gouv.fr): https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales
"""

import tempfile
from pathlib import Path

import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import delete_existing_departments, download_file, ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

CRIME_CSV_URL = (
    "https://static.data.gouv.fr/resources/"
    "bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-"
    "enregistree-par-la-police-et-la-gendarmerie-nationales/"
    "20250710-144817/"
    "donnee-data.gouv-2024-geographie2025-produit-le2025-06-04.csv.gz"
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
    result["departement"] = result["code_commune"].str[:2]

    console.print(f"  -> {len(result)} communes with crime data")
    return result


def run(year: int, departements: list[str]):
    """Main pipeline: download, pivot, and load crime stats (no geom)."""
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

        delete_existing_departments(qualified, departements)

        console.print(f"\n[bold]Loading into {qualified}...[/bold]")
        crime_df.to_sql(table, engine, schema=SCHEMA, if_exists="append", index=False)

        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_code_commune ON {qualified} (code_commune)"
            ))
            conn.commit()

        console.print(f"[green]Done — {len(crime_df)} communes loaded into {qualified}[/green]")
