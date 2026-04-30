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


def aggregate_dvf(dvf_path: Path, departement: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate DVF mutations per cadastral section AND per commune.

    Commune-level metrics use the true median over raw transactions, not a
    weighted average of section medians.
    """
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
    bati["code_commune"] = bati["section_id"].str[:5]

    section_agg = (
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
    section_agg["departement"] = departement

    commune_agg = (
        bati.groupby("code_commune")
        .agg(
            commune_median_price_sqm=("prix_m2", "median"),
            commune_mean_price_sqm=("prix_m2", "mean"),
            commune_sales_count=("id_mutation", "nunique"),
            commune_sections_count=("section_id", "nunique"),
        )
        .reset_index()
    )
    commune_agg["departement"] = departement

    return section_agg, commune_agg


def run(year: int, departements: list[str]):
    """Main pipeline: download, aggregate, and load prices into DB."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    section_table = f"y{year}"
    section_qualified = f"{SCHEMA}.{section_table}"
    commune_table = f"communes_y{year}"
    commune_qualified = f"{SCHEMA}.{commune_table}"

    with tempfile.TemporaryDirectory(prefix="geo-dvf-") as tmpdir:
        tmp = Path(tmpdir)
        section_frames = []
        commune_frames = []

        for dep in departements:
            console.print(f"\n[bold]Department {dep}[/bold]")
            try:
                dvf_url = f"{DVF_BASE_URL}/{year}/departements/{dep}.csv.gz"
                dvf_path = download_file(dvf_url, tmp, label=f"DVF {dep} {year}")

                console.print("  Aggregating DVF prices...")
                section_agg, commune_agg = aggregate_dvf(dvf_path, dep)
                console.print(
                    f"  -> {len(section_agg)} sections, {len(commune_agg)} communes with sales"
                )

                section_frames.append(section_agg)
                commune_frames.append(commune_agg)
            except Exception as e:
                console.print(f"  [red]Skipping {dep}: {e}[/]")

        if not section_frames:
            console.print("[red]No data to load.[/red]")
            return

        final_sections = pd.concat(section_frames, ignore_index=True)
        final_communes = pd.concat(commune_frames, ignore_index=True)

        delete_existing_departments(section_qualified, departements)
        delete_existing_departments(commune_qualified, departements)

        console.print(f"\n[bold]Loading into {section_qualified}...[/bold]")
        final_sections.to_sql(section_table, engine, schema=SCHEMA, if_exists="append", index=False)

        console.print(f"[bold]Loading into {commune_qualified}...[/bold]")
        final_communes.to_sql(commune_table, engine, schema=SCHEMA, if_exists="append", index=False)

        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{section_table}_section_id ON {section_qualified} (section_id)"
            ))
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{commune_table}_code_commune ON {commune_qualified} (code_commune)"
            ))
            conn.commit()

        console.print(
            f"[green]Done — {len(final_sections)} sections and {len(final_communes)} communes "
            f"loaded for {year}[/green]"
        )
