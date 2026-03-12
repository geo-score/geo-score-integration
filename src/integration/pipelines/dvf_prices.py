"""
DVF pipeline — Median prices per cadastral section.

Schema: dvf_prices
Tables: one per year (e.g. dvf_prices.y2023)

Sources:
- DVF open data: https://files.data.gouv.fr/geo-dvf/latest/csv/
- Cadastral sections (Etalab): https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/
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

DVF_BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"
CADASTRE_BASE_URL = (
    "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements"
)

SCHEMA = "dvf_prices"


def _table_name(year: int) -> str:
    return f"y{year}"


def _ensure_schema():
    """Create the dvf_prices schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.commit()


def download_dvf(year: int, dep: str, dest: Path) -> Path:
    """Download DVF CSV for a given department and year."""
    url = f"{DVF_BASE_URL}/{year}/departements/{dep}.csv.gz"
    out = dest / f"dvf_{dep}_{year}.csv.gz"
    if out.exists():
        return out
    console.print(f"  Downloading DVF {dep} {year}...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)
    return out


def download_sections(dep: str, dest: Path) -> Path:
    """Download and decompress cadastral sections GeoJSON."""
    url = f"{CADASTRE_BASE_URL}/{dep}/cadastre-{dep}-sections.json.gz"
    out = dest / f"sections_{dep}.json"
    if out.exists():
        return out
    console.print(f"  Downloading cadastral sections {dep}...")
    gz_path = dest / f"sections_{dep}.json.gz"
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)
    # Decompress for pyogrio/GDAL compatibility
    with gzip.open(gz_path, "rb") as gz, open(out, "wb") as f:
        f.write(gz.read())
    gz_path.unlink()
    return out


def extract_section_id(id_parcelle: str) -> str | None:
    """Extract section ID from id_parcelle (e.g. '75101000AB0001' -> '75101000AB')."""
    if pd.isna(id_parcelle) or len(str(id_parcelle)) < 10:
        return None
    s = str(id_parcelle)
    # id_parcelle = commune(5) + prefixe(3) + section(2) + numero(4)
    return s[:10]


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

    # Keep only sales with a price
    df = df[df["nature_mutation"] == "Vente"].dropna(subset=["valeur_fonciere"])

    df["section_id"] = df["id_parcelle"].map(extract_section_id)
    df = df.dropna(subset=["section_id"])

    # Price per m² for built properties
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
    # section_id = commune(5) + prefixe(3) + code(2) — matches id_parcelle[:10]
    gdf["section_id"] = gdf["commune"] + gdf["prefixe"].fillna("000") + gdf["code"]
    return gdf[["section_id", "geometry"]]


def run(year: int, departements: list[str]):
    """Main pipeline: download, aggregate, and load into DB."""
    ensure_postgis()
    _ensure_schema()

    table = _table_name(year)
    qualified = f"{SCHEMA}.{table}"

    with tempfile.TemporaryDirectory(prefix="geo-dvf-") as tmpdir:
        tmp = Path(tmpdir)
        all_frames = []

        for dep in departements:
            console.print(f"\n[bold]Department {dep}[/bold]")

            # 1. Download data
            dvf_path = download_dvf(year, dep, tmp)
            sections_path = download_sections(dep, tmp)

            # 2. Aggregate DVF by section
            console.print("  Aggregating DVF prices...")
            dvf_agg = aggregate_dvf(dvf_path)
            console.print(f"  -> {len(dvf_agg)} sections with sales")

            # 3. Load geometries
            console.print("  Loading geometries...")
            sections_geom = load_sections_geom(sections_path)

            # 4. Join
            merged = sections_geom.merge(dvf_agg, on="section_id", how="inner")
            merged["departement"] = dep
            console.print(f"  -> {len(merged)} sections with price and geometry")

            all_frames.append(merged)

        if not all_frames:
            console.print("[red]No data to load.[/red]")
            return

        final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True))
        final = final.set_crs(epsg=4326)

        # Delete existing rows for these departments (allows re-run / update)
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

        # Load into database
        console.print(f"\n[bold]Loading into {qualified}...[/bold]")
        final.to_postgis(
            table,
            engine,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            dtype={"geometry": "Geometry"},
        )

        # Convert geometry to native PostGIS column, drop original, add spatial index
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

        console.print(f"[green]Done — {len(final)} sections loaded into {qualified}[/green]")
