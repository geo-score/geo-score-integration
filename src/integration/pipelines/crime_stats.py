"""
Delinquency pipeline — Crime statistics per commune.

Schema: crime_stats
Tables: one per year (e.g. crime_stats.y2024)

Sources:
- Crime stats (data.gouv.fr): https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales
- Commune geometries (Etalab cadastre): https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/
"""

import gzip
import tempfile
from pathlib import Path

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from sqlalchemy import text

from integration.db import engine, ensure_postgis

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

INDICATORS = [
    "Cambriolages de logement",
    "Destructions et dégradations volontaires",
    "Violences physiques intrafamiliales",
    "Violences physiques hors cadre familial",
    "Violences sexuelles",
    "Vols sans violence contre des personnes",
    "Vols violents sans arme",
    "Vols de véhicule",
    "Vols dans les véhicules",
    "Coups et blessures volontaires",
    "Trafic de stupéfiants",
    "Usage de stupéfiants",
    "Escroqueries et fraudes aux moyens de paiement",
]


def _table_name(year: int) -> str:
    return f"y{year}"


def _ensure_schema():
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.commit()


def download_crime_data(dest: Path) -> Path:
    """Download the national commune-level crime CSV."""
    out = dest / "delinquance.csv.gz"
    if out.exists():
        return out
    console.print("  Downloading crime statistics...")
    with httpx.stream("GET", CRIME_CSV_URL, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)
    return out


def download_communes(dep: str, dest: Path) -> Path:
    """Download and decompress commune geometries for a department."""
    url = f"{CADASTRE_BASE_URL}/{dep}/cadastre-{dep}-communes.json.gz"
    out = dest / f"communes_{dep}.json"
    if out.exists():
        return out
    console.print(f"  Downloading commune geometries {dep}...")
    gz_path = dest / f"communes_{dep}.json.gz"
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)
    with gzip.open(gz_path, "rb") as gz, open(out, "wb") as f:
        f.write(gz.read())
    gz_path.unlink()
    return out


def load_crime_data(csv_path: Path, year: int, departements: list[str]) -> pd.DataFrame:
    """Load and pivot crime data: one row per commune, one column per indicator."""
    df = pd.read_csv(
        csv_path,
        sep=";",
        usecols=["CODGEO_2025", "annee", "indicateur", "taux_pour_mille", "nombre"],
        low_memory=False,
    )

    df = df[df["annee"] == year]

    # Filter by department prefix
    dep_prefixes = tuple(departements)
    df = df[df["CODGEO_2025"].str.startswith(dep_prefixes)]

    # Parse numeric values (French decimal separator)
    df["taux_pour_mille"] = (
        df["taux_pour_mille"]
        .astype(str)
        .str.replace(",", ".")
        .apply(pd.to_numeric, errors="coerce")
    )
    df["nombre"] = (
        df["nombre"]
        .astype(str)
        .str.replace(",", ".")
        .apply(pd.to_numeric, errors="coerce")
    )

    # Pivot: one column per indicator (rate per 1000)
    pivot_rate = df.pivot_table(
        index="CODGEO_2025",
        columns="indicateur",
        values="taux_pour_mille",
        aggfunc="first",
    )
    pivot_count = df.pivot_table(
        index="CODGEO_2025",
        columns="indicateur",
        values="nombre",
        aggfunc="first",
    )

    # Clean column names for SQL
    def clean_col(name: str) -> str:
        return (
            name.lower()
            .replace(" ", "_")
            .replace("'", "")
            .replace("é", "e")
            .replace("è", "e")
            .replace("ê", "e")
            .replace("à", "a")
            .replace("û", "u")
            .replace("ô", "o")
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
        path = download_communes(dep, dest)
        gdf = gpd.read_file(path)
        gdf = gdf.rename(columns={"id": "code_commune"})
        frames.append(gdf[["code_commune", "geometry"]])

    combined = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True))
    console.print(f"  -> {len(combined)} commune geometries loaded")
    return combined


def run(year: int, departements: list[str]):
    """Main pipeline: download, pivot, join geometries, and load into DB."""
    ensure_postgis()
    _ensure_schema()

    table = _table_name(year)
    qualified = f"{SCHEMA}.{table}"

    with tempfile.TemporaryDirectory(prefix="geo-delinquance-") as tmpdir:
        tmp = Path(tmpdir)

        # 1. Download crime data
        csv_path = download_crime_data(tmp)

        # 2. Load and pivot
        console.print("\n[bold]Processing crime data...[/bold]")
        crime_df = load_crime_data(csv_path, year, departements)

        if crime_df.empty:
            console.print(f"[red]No crime data found for year {year}.[/red]")
            return

        # 3. Load commune geometries
        console.print("\n[bold]Loading commune geometries...[/bold]")
        communes_geom = load_communes_geom(departements, tmp)

        # 4. Join
        merged = communes_geom.merge(crime_df, on="code_commune", how="inner")
        merged["departement"] = merged["code_commune"].str[:2]
        console.print(f"  -> {len(merged)} communes with data and geometry")

        if merged.empty:
            console.print("[red]No data after join.[/red]")
            return

        final = gpd.GeoDataFrame(merged)
        final = final.set_crs(epsg=4326)

        # 5. Delete existing rows for these departments
        with engine.connect() as conn:
            table_exists = conn.execute(
                text("SELECT to_regclass(:t)"),
                {"t": qualified},
            ).scalar()

            if table_exists:
                dep_list = ",".join(f"'{d}'" for d in departements)
                conn.execute(text(f"DELETE FROM {qualified} WHERE departement IN ({dep_list})"))
                conn.commit()
                console.print(f"  Cleared existing data for departments {departements}")

        # 6. Load into database
        console.print(f"\n[bold]Loading into {qualified}...[/bold]")
        final.to_postgis(
            table,
            engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            dtype={"geometry": "Geometry"},
        )

        # 7. Convert geometry to native PostGIS column, drop original, add spatial index
        with engine.connect() as conn:
            conn.execute(text(
                f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS geom geometry(Geometry, 4326)"
            ))
            conn.execute(text(
                f"UPDATE {qualified} SET geom = geometry::geometry WHERE geom IS NULL"
            ))
            conn.execute(text(
                f"ALTER TABLE {qualified} DROP COLUMN IF EXISTS geometry"
            ))
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {qualified} USING GIST (geom)"
            ))
            conn.commit()

        console.print(f"[green]Done — {len(final)} communes loaded into {qualified}[/green]")
