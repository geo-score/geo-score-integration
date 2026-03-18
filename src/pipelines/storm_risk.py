"""
Storm risk pipeline — Eurocode wind zones + CatNat storm history.

Schema: storm_risk
Tables:
  - wind_zones   : Eurocode EN 1991-1-4 wind zones (4 levels, 19 polygons)
  - catnat_storm  : CatNat storm declarations aggregated per commune

Sources:
  - Eurocode: https://gitlab.com/arep-dev/EC1_GeoJSON (GeoJSON, WGS84)
  - GASPAR CatNat: https://files.georisques.fr/GASPAR/gaspar.zip (CSV)
  - Commune geometries: Etalab cadastre

Wind zones (wind_zone):
  - 1 : V_b0 = 22 m/s (lowest exposure)
  - 2 : V_b0 = 24 m/s
  - 3 : V_b0 = 26 m/s
  - 4 : V_b0 = 28 m/s (highest exposure)
"""

import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import ensure_schema
from common.download import download_file
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "storm_risk"

EUROCODE_URL = (
    "https://gitlab.com/arep-dev/EC1_GeoJSON/-/raw/main/ec1_windCoeff.geojson"
)
GASPAR_URL = "https://files.georisques.fr/GASPAR/gaspar.zip"
COMMUNE_URL = (
    "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/"
    "departements/{dep}/cadastre-{dep}-communes.json.gz"
)

WIND_ZONE_MAP = {22: 1, 24: 2, 26: 3, 28: 4}

STORM_RISK_LABELS = ("Tempête", "Vents Cycloniques")


# ---------------------------------------------------------------------------
# 1. Eurocode wind zones
# ---------------------------------------------------------------------------

def _load_wind_zones(tmp_dir: Path):
    """Download Eurocode wind zones GeoJSON and load into PostGIS."""
    table = "wind_zones"
    qualified = f"{SCHEMA}.{table}"

    console.print("\n[bold cyan]Eurocode wind zones[/]")

    dest = tmp_dir / "ec1_windCoeff.geojson"
    download_file(EUROCODE_URL, tmp_dir, label="Eurocode wind zones")

    gdf = gpd.read_file(dest)
    console.print(f"  {len(gdf)} features loaded")

    gdf["wind_zone"] = gdf["V_B0"].map(WIND_ZONE_MAP)
    gdf["wind_speed_ms"] = gdf["V_B0"]
    gdf = gdf[["wind_zone", "wind_speed_ms", "geometry"]]
    gdf = gdf.rename_geometry("geom")
    gdf = gdf.set_crs(epsg=4326)

    # Truncate and reload (global data, no department partitioning)
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT to_regclass(:t)"), {"t": qualified}
        ).scalar()
        if exists:
            conn.execute(text(f"TRUNCATE TABLE {qualified}"))
            conn.commit()
            console.print("  Truncated existing data")

    gdf.to_postgis(table, engine, schema=SCHEMA, if_exists="append", index=False)

    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_geom "
            f"ON {qualified} USING GIST (geom)"
        ))
        conn.commit()

    console.print(f"  [green]{len(gdf)} wind zone polygons loaded[/]")


# ---------------------------------------------------------------------------
# 2. CatNat storm declarations per commune
# ---------------------------------------------------------------------------

def _load_catnat_storms(tmp_dir: Path, departements: list[str]):
    """Download GASPAR CatNat, aggregate storm declarations per commune."""
    table = "catnat_storm"
    qualified = f"{SCHEMA}.{table}"

    console.print("\n[bold cyan]CatNat storm declarations[/]")

    # --- download & parse GASPAR ---
    download_file(GASPAR_URL, tmp_dir, label="GASPAR")
    zip_path = tmp_dir / "gaspar.zip"

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmp_dir / "gaspar")

    csv_path = tmp_dir / "gaspar" / "catnat_gaspar.csv"
    df = pd.read_csv(csv_path, sep=";", dtype=str, low_memory=False)
    console.print(f"  {len(df):,} total CatNat declarations")

    # Filter storm-related
    df = df[df["lib_risque_jo"].isin(STORM_RISK_LABELS)]
    console.print(f"  {len(df):,} storm-related declarations")

    # Parse dates
    df["dat_deb"] = pd.to_datetime(df["dat_deb"], errors="coerce")
    df["dat_fin"] = pd.to_datetime(df["dat_fin"], errors="coerce")

    # Aggregate by commune
    agg = df.groupby("cod_commune").agg(
        storm_count=("cod_commune", "size"),
        first_event=("dat_deb", "min"),
        last_event=("dat_deb", "max"),
    ).reset_index()
    agg = agg.rename(columns={"cod_commune": "code_commune"})

    # Filter to requested departments (commune code starts with dept code)
    dep_set = set(departements)
    mask = agg["code_commune"].apply(
        lambda c: c[:3] in dep_set if len(c) == 5 and c[:3].isdigit() and c[:3] in dep_set
        else c[:2] in dep_set
    )
    agg = agg[mask]
    console.print(f"  {len(agg):,} communes with storm history")

    if agg.empty:
        console.print("  [yellow]No storm data for these departments[/]")
        return

    # --- delete existing & load with commune geometries ---
    # Delete existing departments
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT to_regclass(:t)"), {"t": qualified}
        ).scalar()
        if exists:
            dep_list = ",".join(f"'{d}'" for d in departements)
            conn.execute(text(
                f"DELETE FROM {qualified} WHERE departement IN ({dep_list})"
            ))
            conn.commit()

    for dep in departements:
        dep_communes = agg[agg["code_commune"].str.startswith(dep)]
        if dep_communes.empty:
            continue

        # Download commune geometries
        url = COMMUNE_URL.format(dep=dep)
        try:
            geom_path = download_file(url, tmp_dir, decompress=True, label=f"communes {dep}")
        except Exception:
            console.print(f"  [yellow]No commune geometries for {dep}[/]")
            continue

        communes_gdf = gpd.read_file(geom_path)
        communes_gdf = communes_gdf[["id", "geometry"]].rename(columns={"id": "code_commune"})

        merged = communes_gdf.merge(dep_communes, on="code_commune", how="inner")
        if merged.empty:
            continue

        merged["departement"] = dep
        merged["first_event"] = merged["first_event"].dt.strftime("%Y-%m-%d")
        merged["last_event"] = merged["last_event"].dt.strftime("%Y-%m-%d")

        gdf = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")
        gdf = gdf.rename_geometry("geom")

        gdf.to_postgis(table, engine, schema=SCHEMA, if_exists="append", index=False)
        console.print(f"  [green]{dep}: {len(gdf)} communes loaded[/]")

    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_geom "
            f"ON {qualified} USING GIST (geom)"
        ))
        conn.commit()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        _load_wind_zones(tmp_dir)
        _load_catnat_storms(tmp_dir, departements)

    console.print(f"\n[bold green]Done — storm risk data loaded into {SCHEMA}.*[/]")
