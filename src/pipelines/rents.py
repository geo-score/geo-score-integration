"""
Rents pipeline — Carte des loyers (Ministère de la Transition écologique).

Schema: rents
Tables: one per year (e.g. rents.indicators_2025)

Estimates per commune for 4 property categories:
- apartment_all (52 m² reference)
- apartment_t1_t2 (37 m²)
- apartment_t3_plus (72 m²)
- house (92 m²)

Source: https://www.data.gouv.fr/datasets/carte-des-loyers-indicateurs-de-loyers-dannonce-par-commune-en-{year}/
"""

import tempfile
from pathlib import Path

import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import download_file, ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "rents"
DATA_GOUV_RESOURCE_BASE = "https://www.data.gouv.fr/api/1/datasets/r"

# (year, type_local) -> data.gouv resource ID
RESOURCES: dict[int, dict[str, str]] = {
    2022: {
        "apartment_all":     "bc9d5d13-07cc-4d38-8254-88db065bd42b",
        "apartment_t1_t2":   "7141612b-8029-44a4-a048-921a85a47b1f",
        "apartment_t3_plus": "b398ede4-75f9-47ac-bfc5-d912c0012880",
        "house":             "dfb542cd-a808-41e2-9157-8d39b5c24edb",
    },
    2023: {
        "apartment_all":     "43618998-3b37-4a69-bb25-f321f1a93ed1",
        "apartment_t1_t2":   "edadefbc-9707-45ef-a841-283608709e58",
        "apartment_t3_plus": "08871624-ccb5-457a-83d5-fb134cba60da",
        "house":             "34434cef-2f85-43b9-a601-c625ee426cb7",
    },
    2024: {
        "apartment_all":     "64c6e452-783a-4d71-95e6-22b1cdf96d37",
        "apartment_t1_t2":   "89956da9-5b9b-41d7-8703-18dbec4d54a2",
        "apartment_t3_plus": "b7f5522e-d7a9-4861-8b52-f0ed4c088944",
        "house":             "b3ec6ed0-1cb7-477e-bfd4-2aa370333994",
    },
    2025: {
        "apartment_all":     "55b34088-0964-415f-9df7-d87dd98a09be",
        "apartment_t1_t2":   "14a1fe11-b2d1-49b3-9f6b-83d12df9482c",
        "apartment_t3_plus": "5e3b28a4-cf56-43a3-ae79-43cceeb27f8c",
        "house":             "129f764d-b613-44e4-952c-5ff50a8c9b73",
    },
}


def parse_csv(csv_path: Path, type_local: str) -> pd.DataFrame:
    """Parse a Carte des loyers CSV and normalize columns."""
    df = pd.read_csv(
        csv_path, sep=";", decimal=",", encoding="latin-1",
        dtype={"INSEE_C": str, "DEP": str},
    )
    df = df.rename(columns={
        "INSEE_C": "code_commune",
        "DEP": "departement",
        "loypredm2": "loyer_m2_median",
        "lwr.IPm2": "loyer_m2_lower",
        "upr.IPm2": "loyer_m2_upper",
        "TYPPRED": "prediction_type",
        "nbobs_com": "nb_obs",
        "R2_adj": "r2_adj",
    })
    df["type_local"] = type_local
    return df[[
        "code_commune", "type_local", "departement",
        "loyer_m2_median", "loyer_m2_lower", "loyer_m2_upper",
        "prediction_type", "nb_obs", "r2_adj",
    ]]


def run(year: int, departements: list[str]):
    """Main pipeline: download all 4 CSVs for the year, normalize, load."""
    if year not in RESOURCES:
        available = ", ".join(str(y) for y in sorted(RESOURCES))
        raise ValueError(f"No rent resources mapped for {year}. Available: {available}")

    ensure_postgis()
    ensure_schema(SCHEMA)

    table = f"indicators_{year}"
    qualified = f"{SCHEMA}.{table}"

    with tempfile.TemporaryDirectory(prefix="geo-rents-") as tmpdir:
        tmp = Path(tmpdir)
        frames = []

        for type_local, resource_id in RESOURCES[year].items():
            url = f"{DATA_GOUV_RESOURCE_BASE}/{resource_id}"
            path = download_file(url, tmp, label=f"rents {year} {type_local}", filename=f"{type_local}.csv")
            df = parse_csv(path, type_local)
            console.print(f"  {type_local}: {len(df)} communes")
            frames.append(df)

        all_df = pd.concat(frames, ignore_index=True)

        if departements:
            all_df = all_df[all_df["departement"].isin(departements)]
            console.print(f"  Filtered to {len(departements)} departments → {len(all_df)} rows")

        deps_to_clear = sorted(all_df["departement"].dropna().unique().tolist())
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:t)"), {"t": qualified}
            ).scalar()
            if exists and deps_to_clear:
                dep_list = ",".join(f"'{d}'" for d in deps_to_clear)
                conn.execute(text(
                    f"DELETE FROM {qualified} WHERE departement IN ({dep_list})"
                ))
                conn.commit()
                console.print(f"  Cleared existing rents for {len(deps_to_clear)} departments")

        console.print(f"\n[bold]Loading into {qualified}...[/bold]")
        all_df.to_sql(table, engine, schema=SCHEMA, if_exists="append", index=False)

        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_code_commune ON {qualified} (code_commune)"
            ))
            conn.commit()

        console.print(f"[green]Done — {len(all_df)} rows loaded into {qualified}[/green]")
