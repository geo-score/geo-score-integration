"""
DVF pipeline — Median prices per cadastral section.

Schema: dvf_prices
Tables: one per year (e.g. dvf_prices.y2023)

Geometries live in the reference table `dvf_prices.sections` (loaded by `dvf-sections`).
Yearly tables only store aggregated price metrics and join on `section_id`.

Source:
- DVF open data: https://files.data.gouv.fr/geo-dvf/latest/csv/
"""

import tempfile
from pathlib import Path

import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import delete_existing_departments, download_file, ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

DVF_BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"

SCHEMA = "dvf_prices"


def extract_section_id(id_parcelle: str) -> str | None:
    """Extract section ID from id_parcelle (e.g. '75101000AB0001' -> '75101000AB')."""
    if pd.isna(id_parcelle) or len(str(id_parcelle)) < 10:
        return None
    return str(id_parcelle)[:10]


def aggregate_dvf(dvf_path: Path, departement: str) -> pd.DataFrame:
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

    df = df[
        df["nature_mutation"].isin(
            ["Vente", "Vente en l'état futur d'achèvement", "Adjudication"]
        )
    ].dropna(subset=["valeur_fonciere"])
    df["section_id"] = df["id_parcelle"].map(extract_section_id)
    df = df.dropna(subset=["section_id"])

    bati = df[df["surface_reelle_bati"] > 0].copy()
    bati["prix_m2"] = bati["valeur_fonciere"] / bati["surface_reelle_bati"]

    agg = (
        bati.groupby("section_id")
        .agg(
            prix_m2_q1=("prix_m2", lambda s: s.quantile(0.25)),
            prix_m2_median=("prix_m2", "median"),
            prix_m2_q3=("prix_m2", lambda s: s.quantile(0.75)),
            prix_m2_mean=("prix_m2", "mean"),
            nb_ventes=("id_mutation", "nunique"),
            surface_mediane=("surface_reelle_bati", "median"),
        )
        .reset_index()
    )
    agg["departement"] = departement
    return agg


def run(year: int, departements: list[str]):
    """Main pipeline: download, aggregate, and load prices into DB."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    table = f"y{year}"
    qualified = f"{SCHEMA}.{table}"

    with tempfile.TemporaryDirectory(prefix="geo-dvf-") as tmpdir:
        tmp = Path(tmpdir)
        all_frames = []

        for dep in departements:
            console.print(f"\n[bold]Department {dep}[/bold]")
            try:
                dvf_url = f"{DVF_BASE_URL}/{year}/departements/{dep}.csv.gz"
                dvf_path = download_file(dvf_url, tmp, label=f"DVF {dep} {year}")

                console.print("  Aggregating DVF prices...")
                dvf_agg = aggregate_dvf(dvf_path, dep)
                console.print(f"  -> {len(dvf_agg)} sections with sales")

                all_frames.append(dvf_agg)
            except Exception as e:
                console.print(f"  [red]Skipping {dep}: {e}[/]")

        if not all_frames:
            console.print("[red]No data to load.[/red]")
            return

        final = pd.concat(all_frames, ignore_index=True)

        delete_existing_departments(qualified, departements)

        console.print(f"\n[bold]Loading into {qualified}...[/bold]")
        final.to_sql(table, engine, schema=SCHEMA, if_exists="append", index=False)

        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_section_id ON {qualified} (section_id)"
            ))
            conn.commit()

        console.print(f"[green]Done — {len(final)} sections loaded into {qualified}[/green]")
