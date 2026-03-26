"""
Climate pipeline — Météo-France monthly climate data per station.

Schema: climate
Table: stations

Source: Météo-France open data (data.gouv.fr)
        Dataset: Données climatologiques de base - mensuelles
"""

import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import Point

from common import ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "climate"
TABLE = "stations"

BASE_URL = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/MENS"


def _download_dept(dep: str, tmp: Path) -> Path | None:
    import httpx

    url = f"{BASE_URL}/MENSQ_{dep}_previous-1950-2024.csv.gz"
    dest = tmp / f"meteo_{dep}.csv.gz"

    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        return dest
    except Exception as e:
        console.print(f"  [red]Download failed: {e}[/]")
        return None


def _process_dept(path: Path, dep: str) -> gpd.GeoDataFrame:
    df = pd.read_csv(path, sep=";", compression="gzip", low_memory=False)

    # Filter to recent years (last 10 years)
    df["year"] = df["AAAAMM"].astype(str).str[:4].astype(int)
    df = df[df["year"] >= 2014]

    # Aggregate per station: avg temps, max canicule days
    agg = df.groupby(["NUM_POSTE", "NOM_USUEL", "LAT", "LON", "ALTI"]).agg(
        avg_temp_max=("TX", "mean"),
        avg_temp_min=("TN", "mean"),
        max_temp_recorded=("TXAB", "max"),
        total_days_above_30=("NBJTX30", "sum"),
        total_days_above_35=("NBJTX35", "sum"),
        total_days_above_25=("NBJTX25", "sum"),
        total_months=("AAAAMM", "count"),
    ).reset_index()

    # Annualize
    agg["years_count"] = agg["total_months"] / 12
    for col in ["total_days_above_30", "total_days_above_35", "total_days_above_25"]:
        agg[f"avg_{col.replace('total_', '')}"] = (agg[col] / agg["years_count"]).round(1)

    agg = agg.rename(columns={
        "NUM_POSTE": "station_id",
        "NOM_USUEL": "station_name",
        "LAT": "lat",
        "LON": "lon",
        "ALTI": "altitude",
    })

    # Filter valid coords
    agg = agg.dropna(subset=["lat", "lon"])
    agg = agg[(agg["lat"] != 0) & (agg["lon"] != 0)]

    geometry = [Point(lon, lat) for lon, lat in zip(agg["lon"], agg["lat"])]
    gdf = gpd.GeoDataFrame(agg, geometry=geometry, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")
    gdf["departement"] = dep

    # Keep only useful columns
    keep = [
        "station_id", "station_name", "altitude", "departement",
        "avg_temp_max", "avg_temp_min", "max_temp_recorded",
        "avg_days_above_25", "avg_days_above_30", "avg_days_above_35",
        "geom",
    ]
    return gdf[[c for c in keep if c in gdf.columns]]


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    all_frames = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        for i, dep in enumerate(departements):
            console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")
            try:
                path = _download_dept(dep, tmp_dir)
                if not path:
                    continue

                gdf = _process_dept(path, dep)
                console.print(f"  -> {len(gdf)} stations with climate data")
                if not gdf.empty:
                    all_frames.append(gdf)
            except Exception as e:
                console.print(f"  [red]Skipping {dep}: {e}[/]")

    if not all_frames:
        console.print("[red]No data to load.[/red]")
        return

    final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True)).set_crs(epsg=4326)

    qualified = f"{SCHEMA}.{TABLE}"
    console.print(f"\n[bold]Loading into {qualified}...[/bold]")

    final.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)

    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {SCHEMA}.{TABLE} USING GIST (geom)"))
        conn.commit()

    console.print(f"[green]Done — {len(final):,} stations loaded into {qualified}[/green]")
