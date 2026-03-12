import typer
from rich.console import Console

app = typer.Typer(name="geo-integrate", help="Load geospatial datasets into geo-score DB.")
console = Console()


@app.command()
def dvf(
    year: int = typer.Option(2023, help="DVF year to load"),
    departements: list[str] = typer.Option(
        ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
    ),
):
    """Load median DVF prices per cadastral section."""
    from integration.pipelines.dvf_prices import run

    run(year=year, departements=departements)


@app.command()
def delinquance(
    year: int = typer.Option(2024, help="Year to load"),
    departements: list[str] = typer.Option(
        ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
    ),
):
    """Load crime statistics per commune."""
    from integration.pipelines.crime_stats import run

    run(year=year, departements=departements)


@app.command()
def shops(
    departements: list[str] = typer.Option(
        ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
    ),
    snapshot: str = typer.Option(None, help="Snapshot date (YYYY-MM-DD), defaults to today"),
):
    """Load OSM shops and amenities as points."""
    from datetime import date as dt

    from integration.pipelines.osm_shops import run

    snap = dt.fromisoformat(snapshot) if snapshot else None
    run(departements=departements, snapshot=snap)


@app.command()
def check_db():
    """Check database connection."""
    from sqlalchemy import text

    from integration.db import engine

    with engine.connect() as conn:
        result = conn.execute(text("SELECT PostGIS_Version()")).scalar()
        console.print(f"[green]Connected — PostGIS {result}[/green]")


if __name__ == "__main__":
    app()
