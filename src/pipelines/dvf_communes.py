"""
DVF commune-level aggregation pipeline — pre-aggregated metrics per commune.

Schema: dvf_prices
Table: communes_y{year}

Adds the commune-level pre-aggregation without touching the existing
section-level table (`dvf_prices.y{year}`). Use this when you've already
loaded sections via `dvf` and only need to enrich with commune metrics.

Source:
- DVF open data: https://files.data.gouv.fr/geo-dvf/latest/csv/
"""

import tempfile
from pathlib import Path

import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import delete_existing_departments, download_file, ensure_schema
from pipelines.dvf_prices import DVF_BASE_URL, SCHEMA, aggregate_dvf
from settings.db import engine, ensure_postgis

console = Console()


def run(year: int, departements: list[str]):
    """Load commune-level DVF pre-aggregation only."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    table = f"communes_y{year}"
    qualified = f"{SCHEMA}.{table}"

    with tempfile.TemporaryDirectory(prefix="geo-dvf-communes-") as tmpdir:
        tmp = Path(tmpdir)
        all_frames = []

        for dep in departements:
            console.print(f"\n[bold]Department {dep}[/bold]")
            try:
                dvf_url = f"{DVF_BASE_URL}/{year}/departements/{dep}.csv.gz"
                dvf_path = download_file(dvf_url, tmp, label=f"DVF {dep} {year}")

                console.print("  Aggregating DVF prices at commune level...")
                _, commune_agg = aggregate_dvf(dvf_path, dep)
                console.print(f"  -> {len(commune_agg)} communes with sales")

                all_frames.append(commune_agg)
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
                f"CREATE INDEX IF NOT EXISTS idx_{table}_code_commune ON {qualified} (code_commune)"
            ))
            conn.commit()

        console.print(f"[green]Done — {len(final)} communes loaded into {qualified}[/green]")
