"""
Air quality pipeline — ATMO index per commune from Atmo France.

Schema: climate
Table: air_quality

Source: Atmo France via data.gouv.fr API
        Indice de la qualité de l'air quotidien par commune
"""

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from shapely.geometry import Point
from sqlalchemy import text

from common import ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "climate"
TABLE = "air_quality"

# Atmo France API for the latest ATMO indices
ATMO_API = "https://services9.arcgis.com/7Sr9Ek9c1QTKmbwr/arcgis/rest/services"
STATIONS_URL = f"{ATMO_API}/Mesure_horaire_(30j)/FeatureServer/0/query"
INDEX_URL = f"{ATMO_API}/ind_atmo_com/FeatureServer/0/query"


def _fetch_atmo_index() -> pd.DataFrame:
    """Fetch latest ATMO index per commune."""
    all_rows = []
    offset = 0
    page_size = 2000

    while True:
        params = {
            "where": "1=1",
            "outFields": "code_zone,lib_zone,date_ech,code_qual,lib_qual,source,type_zone,partition_field",
            "f": "json",
            "resultRecordCount": page_size,
            "resultOffset": offset,
            "orderByFields": "code_zone",
        }

        try:
            resp = httpx.get(INDEX_URL, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            console.print(f"  [red]API error at offset {offset}: {e}[/]")
            break

        features = data.get("features", [])
        if not features:
            break

        for f in features:
            attrs = f.get("attributes", {})
            all_rows.append(attrs)

        offset += page_size
        if len(features) < page_size:
            break

        if offset % 10000 == 0:
            console.print(f"  ... {len(all_rows):,} records fetched")

    return pd.DataFrame(all_rows)


def _fetch_commune_centroids(codes: list[str]) -> dict[str, tuple[float, float]]:
    """Get commune centroids from existing crime_stats or dvf data."""
    # Use communes from crime_stats which have geometry
    try:
        from sqlalchemy import create_engine
        result = engine.connect().execute(text("""
            SELECT code_commune, ST_Y(ST_Centroid(geom)) as lat, ST_X(ST_Centroid(geom)) as lon
            FROM crime_stats.y2024
        """))
        return {row[0]: (row[1], row[2]) for row in result}
    except Exception:
        return {}


def run(departements: list[str] | None = None):
    ensure_postgis()
    ensure_schema(SCHEMA)

    console.print("\n[bold]Fetching ATMO air quality index...[/bold]")
    df = _fetch_atmo_index()
    console.print(f"  -> {len(df):,} records fetched")

    if df.empty:
        console.print("[red]No data.[/red]")
        return

    # Keep latest date per commune
    df = df.sort_values("date_ech", ascending=False).drop_duplicates(subset=["code_zone"], keep="first")
    console.print(f"  -> {len(df):,} communes with latest index")

    # Get commune centroids for geocoding
    console.print("  Fetching commune centroids...")
    centroids = _fetch_commune_centroids(df["code_zone"].tolist())
    console.print(f"  -> {len(centroids):,} commune centroids available")

    # Merge
    df["lat"] = df["code_zone"].map(lambda c: centroids.get(c, (None, None))[0])
    df["lon"] = df["code_zone"].map(lambda c: centroids.get(c, (None, None))[1])
    df = df.dropna(subset=["lat", "lon"])

    df = df.rename(columns={
        "code_zone": "commune_code",
        "lib_zone": "commune_name",
        "code_qual": "quality_index",
        "lib_qual": "quality_label",
        "date_ech": "date",
    })

    geometry = [Point(lon, lat) for lon, lat in zip(df["lon"], df["lat"])]
    gdf = gpd.GeoDataFrame(
        df[["commune_code", "commune_name", "quality_index", "quality_label", "date"]],
        geometry=geometry,
        crs="EPSG:4326",
    )
    gdf = gdf.rename_geometry("geom")

    if departements:
        gdf = gdf[gdf["commune_code"].str[:2].isin(departements) | gdf["commune_code"].str[:3].isin(departements)]

    qualified = f"{SCHEMA}.{TABLE}"
    console.print(f"\n[bold]Loading {len(gdf):,} records into {qualified}...[/bold]")

    gdf.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {SCHEMA}.{TABLE} USING GIST (geom)"))
        conn.commit()

    console.print(f"[green]Done — {len(gdf):,} air quality records loaded into {qualified}[/green]")
