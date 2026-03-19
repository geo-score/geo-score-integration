"""
BDNB pipeline — Base de Données Nationale des Bâtiments.

Schema: bati
Tables: buildings, construction, energy_dpe, natural_risks, property_values,
        coproperty, social_housing, addresses

Source: BDNB open data (CSTB)
        https://open-data.s3.fr-par.scw.cloud/bdnb_millesime_2025-07-a/

Downloads per-department CSV ZIPs, loads selected tables into PostGIS.
The batiment_groupe table contains geometry (WKT); other tables join via
batiment_groupe_id.
"""

import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely import wkt
from sqlalchemy import text

from common import delete_existing_departments, ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "bati"
MILLESIME = "2025-07-a"

S3_BASE = "https://open-data.s3.fr-par.scw.cloud/bdnb_millesime_{v}/millesime_{v}_dep{dep}"
ZIP_URL = S3_BASE + "/open_data_millesime_{v}_dep{dep}_csv.zip"

# Tables to load: (csv_name, target_table, has_geometry)
TABLES = [
    ("batiment_groupe", "buildings", True),
    ("batiment_groupe_ffo_bat", "construction", False),
    ("batiment_groupe_dpe_representatif_logement", "energy_dpe", False),
    ("batiment_groupe_risques", "natural_risks", False),
    ("batiment_groupe_dvf_open_representatif", "property_values", False),
    ("batiment_groupe_rnc", "coproperty", False),
    ("batiment_groupe_rpls", "social_housing", False),
    ("adresse", "addresses", True),
]


def _download_dep(dep: str, tmp_dir: Path) -> Path | None:
    """Download BDNB CSV ZIP for a department."""
    import httpx

    # BDNB uses lowercase for Corsica
    dep_key = dep.lower()
    url = ZIP_URL.format(v=MILLESIME, dep=dep_key)
    dest = tmp_dir / f"bdnb_{dep}.zip"

    if dest.exists():
        return dest

    console.print(f"  Downloading BDNB {dep}...")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=65536):
                    f.write(chunk)
        return dest
    except httpx.HTTPStatusError as e:
        console.print(f"  [red]Download failed: {e.response.status_code}[/]")
        return None


def _find_csv(zf: zipfile.ZipFile, table_name: str) -> str | None:
    """Find a CSV file matching a table name inside a ZIP."""
    for name in zf.namelist():
        if name.endswith(f"{table_name}.csv"):
            return name
    return None


def _detect_separator(zf: zipfile.ZipFile, csv_name: str) -> str:
    """Detect CSV separator (comma or semicolon)."""
    with zf.open(csv_name) as f:
        header = f.readline().decode("utf-8")
    return ";" if ";" in header else ","


def _fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Fix columns where pandas infers float due to NaN but the data is integer/string."""
    for col in df.columns:
        if col.endswith("_insee") or col == "code_iris":
            df[col] = df[col].astype("Int64").astype("string").where(df[col].notna())
    return df


def _load_geo_table(
    zf: zipfile.ZipFile, csv_name: str, table: str, dep: str, sep: str
):
    """Load a CSV with WKT geometry into PostGIS."""
    df = pd.read_csv(zf.open(csv_name), sep=sep, low_memory=False, dtype=str)

    # Find WKT geometry column
    geom_col = None
    for col in df.columns:
        if col.upper() == "WKT" or "geom" in col.lower():
            geom_col = col
            break

    if geom_col is None or df.empty:
        console.print(f"    [yellow]No geometry in {table}[/]")
        return 0

    # Parse WKT to geometry
    valid = df[geom_col].notna() & (df[geom_col] != "")
    df = df[valid]

    geometry = gpd.GeoSeries(df[geom_col].apply(wkt.loads), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df.drop(columns=[geom_col]), geometry=geometry, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")

    gdf.to_postgis(table, engine, schema=SCHEMA, if_exists="append", index=False)
    return len(gdf)


def _load_flat_table(
    zf: zipfile.ZipFile, csv_name: str, table: str, dep: str, sep: str
):
    """Load a CSV without geometry into PostgreSQL."""
    df = pd.read_csv(zf.open(csv_name), sep=sep, low_memory=False, dtype=str)

    if df.empty:
        return 0

    df.to_sql(table, engine, schema=SCHEMA, if_exists="append", index=False)
    return len(df)


def _ensure_indexes():
    """Create spatial + btree indexes (once, idempotent)."""
    with engine.connect() as conn:
        # Spatial indexes on geo tables
        for table in ("buildings", "addresses"):
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_geom "
                f"ON {SCHEMA}.{table} USING GIST (geom)"
            ))

        # Btree on batiment_groupe_id for joins
        for _, table, _ in TABLES:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_bg_id "
                f"ON {SCHEMA}.{table} (batiment_groupe_id)"
            ))

        # Department index for idempotent reloads
        for _, table, _ in TABLES:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_dep "
                f"ON {SCHEMA}.{table} (code_departement_insee)"
            ))

        conn.commit()


def _process_department(dep: str, tmp_dir: Path):
    """Download and load all BDNB tables for one department."""
    zip_path = _download_dep(dep, tmp_dir)
    if not zip_path:
        return

    with zipfile.ZipFile(zip_path) as zf:
        for csv_table, target_table, has_geom in TABLES:
            csv_name = _find_csv(zf, csv_table)
            if not csv_name:
                console.print(f"    [yellow]{csv_table} not found[/]")
                continue

            sep = _detect_separator(zf, csv_name)

            if has_geom:
                count = _load_geo_table(zf, csv_name, target_table, dep, sep)
            else:
                count = _load_flat_table(zf, csv_name, target_table, dep, sep)

            console.print(f"    {target_table}: {count:,} rows")


def run(departements: list[str], *, reset: bool = False):
    ensure_postgis()
    ensure_schema(SCHEMA)

    # Drop all tables if reset requested (useful after schema changes)
    if reset:
        with engine.connect() as conn:
            for _, table, _ in TABLES:
                conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{table} CASCADE"))
            conn.commit()
        console.print("  [yellow]Reset: all bati tables dropped[/]")

    # Delete existing department data
    for _, table, _ in TABLES:
        qualified = f"{SCHEMA}.{table}"
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:t)"), {"t": qualified}
            ).scalar()
            if exists:
                dep_list = ",".join(f"'{d}'" for d in departements)
                conn.execute(text(
                    f"DELETE FROM {qualified} WHERE code_departement_insee IN ({dep_list})"
                ))
                conn.commit()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        for i, dep in enumerate(departements):
            console.print(
                f"\n[bold cyan]Department {dep} ({i + 1}/{len(departements)})[/]"
            )
            _process_department(dep, tmp_dir)

    console.print("\n  Creating indexes...")
    _ensure_indexes()
    console.print(f"\n[bold green]Done — BDNB loaded into {SCHEMA}.*[/]")
