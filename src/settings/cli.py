import typer
from rich.console import Console

app = typer.Typer(name="geo-integrate", help="Load geospatial datasets into geo-score DB.")
console = Console()

ALL_DEPS = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "2A", "2B",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95",
    "971", "972", "973", "974", "976",
]


def _resolve_deps(departements: list[str], all_deps: bool) -> list[str]:
    return ALL_DEPS if all_deps else departements


@app.command()
def dvf(
        year: int = typer.Option(2023, help="DVF year to load"),
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load median DVF prices per cadastral section."""
    from pipelines.dvf_prices import run

    run(year=year, departements=_resolve_deps(departements, all_deps))


@app.command()
def delinquance(
        year: int = typer.Option(2024, help="Year to load"),
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load crime statistics per commune."""
    from pipelines.crime_stats import run

    run(year=year, departements=_resolve_deps(departements, all_deps))


@app.command()
def shops(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load OSM shops and amenities as points."""
    from pipelines.osm_shops import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def green_spaces(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load OSM green spaces (parks, gardens, playgrounds) as polygons."""
    from pipelines.osm_green_spaces import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def exposition(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load MNT sun exposure (aspect classification from Copernicus DEM 90m)."""
    from pipelines.mnt_exposure import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def flood_tri(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load TRI flood zones (Directive Inondation 2020)."""
    from pipelines.flood_tri import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def clay_risk(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load RGA clay shrink-swell risk zones (retrait-gonflement des argiles)."""
    from pipelines.clay_risk import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def storm_risk(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load storm risk: Eurocode wind zones + CatNat storm history per commune."""
    from pipelines.storm_risk import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def bdnb(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
        reset: bool = typer.Option(False, "--reset", help="Drop and recreate all bati tables"),
):
    """Load BDNB building data (energy, risks, DVF, copropriete)."""
    from pipelines.bdnb import run

    run(departements=_resolve_deps(departements, all_deps), reset=reset)


@app.command()
def transport(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load OSM transport stops (train stations, metro, tram, bus)."""
    from pipelines.osm_transport import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def roads(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load OSM road network and parking locations."""
    from pipelines.osm_roads import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def dpe_collectif(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load DPE collectifs from ADEME (building-level energy performance)."""
    from pipelines.dpe_collectif import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def water(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load OSM water bodies (rivers, lakes, canals)."""
    from pipelines.osm_water import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def climate(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load Météo-France climate data (temperatures, heat days per station)."""
    from pipelines.climate import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def coastal_erosion():
    """Load coastal erosion indicators from Cerema/Géolittoral."""
    from pipelines.coastal_erosion import run

    run()


@app.command()
def icu():
    """Load Urban Heat Island (ICU) indicators from CSTB."""
    from pipelines.icu import run

    run()


@app.command()
def air_quality():
    """Load ATMO air quality index per commune."""
    from pipelines.air_quality import run

    run()


@app.command()
def pollens():
    """Load ATMO pollen index per commune."""
    from pipelines.pollens import run

    run()


@app.command()
def wiki_pois(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Enrich OSM POIs with Wikidata descriptions, images, and Wikipedia extracts."""
    from pipelines.wiki_pois import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def plu(
        departements: list[str] = typer.Option(
            ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
        ),
        all_deps: bool = typer.Option(False, "--all", help="Load all departments"),
):
    """Load PLU zones and prescriptions from Géoportail de l'Urbanisme."""
    from pipelines.plu import run

    run(departements=_resolve_deps(departements, all_deps))


@app.command()
def check_db():
    """Check database connection."""
    from sqlalchemy import text

    from settings.db import engine

    with engine.connect() as conn:
        result = conn.execute(text("SELECT PostGIS_Version()")).scalar()
        console.print(f"[green]Connected — PostGIS {result}[/green]")


if __name__ == "__main__":
    app()
