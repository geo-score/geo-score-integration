"""
Air quality pipeline — ATMO index per commune from Atmo France API v2.

Schema: climate
Table: air_quality

Source: Atmo France API v2
        https://admindata.atmo-france.org/api/doc/v2
        Requires JWT authentication (24h token).
"""

from datetime import date

import geopandas as gpd
import httpx
from rich.console import Console
from shapely.geometry import Point
from sqlalchemy import text

from common import ensure_schema
from settings.config import settings
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "climate"
TABLE = "air_quality"

ATMO_BASE = "https://admindata.atmo-france.org"
ATMO_LOGIN = f"{ATMO_BASE}/api/login"
ATMO_INDEX = f"{ATMO_BASE}/api/v2/data/indices/atmo"


def _get_token() -> str:
    """Authenticate and get JWT token."""
    username = settings.atmo_username
    password = settings.atmo_password
    if not username or not password:
        raise ValueError("ATMO_USERNAME and ATMO_PASSWORD must be set in .env")

    resp = httpx.post(
        ATMO_LOGIN,
        json={"username": username, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise ValueError("No token in login response")
    console.print("  [green]Authenticated with ATMO France[/]")
    return token


def _fetch_indices(token: str, target_date: str) -> gpd.GeoDataFrame:
    """Fetch ATMO indices for a given date as GeoJSON."""
    resp = httpx.get(
        ATMO_INDEX,
        params={"date": target_date},
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("features"):
        return gpd.GeoDataFrame()

    # Parse GeoJSON features
    rows = []
    for f in data["features"]:
        props = f.get("properties", {})
        # Use WGS84 coords from properties (geometry is in EPSG:3857)
        lat = props.get("y_wgs84")
        lon = props.get("x_wgs84")
        if lat is None or lon is None:
            continue

        rows.append({
            "commune_code": props.get("code_zone"),
            "commune_name": props.get("lib_zone"),
            "quality_index": props.get("code_qual"),
            "quality_label": props.get("lib_qual"),
            "no2_index": props.get("code_no2"),
            "o3_index": props.get("code_o3"),
            "pm10_index": props.get("code_pm10"),
            "pm25_index": props.get("code_pm25"),
            "so2_index": props.get("code_so2"),
            "source": props.get("source"),
            "date": props.get("date_ech"),
            "geom": Point(lon, lat),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


def run(departements: list[str] | None = None):
    ensure_postgis()
    ensure_schema(SCHEMA)

    console.print("\n[bold]ATMO Air Quality Index[/bold]")

    token = _get_token()
    target = date.today().isoformat()
    console.print(f"  Fetching indices for {target}...")

    gdf = _fetch_indices(token, target)
    console.print(f"  -> {len(gdf):,} commune indices fetched")

    if gdf.empty:
        console.print("[red]No data.[/red]")
        return

    if departements:
        gdf = gdf[
            gdf["commune_code"].str[:2].isin(departements)
            | gdf["commune_code"].str[:3].isin(departements)
        ]
        console.print(f"  -> {len(gdf):,} after department filter")

    qualified = f"{SCHEMA}.{TABLE}"
    console.print(f"\n[bold]Loading into {qualified}...[/bold]")

    gdf.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)

    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {SCHEMA}.{TABLE} USING GIST (geom)"
        ))
        conn.commit()

    console.print(f"[green]Done — {len(gdf):,} air quality records loaded into {qualified}[/green]")
