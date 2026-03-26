"""
Pollens pipeline — Pollen index per commune from Atmo France API v2.

Schema: climate
Table: pollens

Source: Atmo France API v2
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
TABLE = "pollens"

ATMO_BASE = "https://admindata.atmo-france.org"
ATMO_LOGIN = f"{ATMO_BASE}/api/login"
ATMO_POLLENS = f"{ATMO_BASE}/api/v2/data/indices/pollens"


def _get_token() -> str:
    resp = httpx.post(
        ATMO_LOGIN,
        json={"username": settings.atmo_username, "password": settings.atmo_password},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def _get_commune_coords() -> dict[str, tuple[float, float]]:
    """Get commune centroids from air_quality table (already geocoded)."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT commune_code, ST_Y(geom) AS lat, ST_X(geom) AS lon FROM climate.air_quality"
            ))
            return {row[0]: (row[1], row[2]) for row in result}
    except Exception:
        return {}


def _fetch_pollens(token: str, target_date: str) -> gpd.GeoDataFrame:
    resp = httpx.get(
        ATMO_POLLENS,
        params={"date": target_date},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()

    console.print("  Fetching commune coordinates...")
    coords = _get_commune_coords()
    console.print(f"  -> {len(coords):,} commune coords available")

    rows = []
    for f in data.get("features", []):
        props = f.get("properties") or {}
        code = props.get("code_zone")
        if not code or code not in coords:
            continue

        lat, lon = coords[code]

        rows.append({
            "commune_code": code,
            "commune_name": props.get("lib_zone"),
            "quality_index": props.get("code_qual"),
            "quality_label": props.get("lib_qual"),
            "alert": props.get("alerte"),
            "responsible_pollen": props.get("pollen_resp"),
            "birch_index": props.get("code_boul"),
            "grass_index": props.get("code_gram"),
            "olive_index": props.get("code_oliv"),
            "ragweed_index": props.get("code_ambr"),
            "mugwort_index": props.get("code_arm"),
            "alder_index": props.get("code_aul"),
            "birch_conc": props.get("conc_boul"),
            "grass_conc": props.get("conc_gram"),
            "olive_conc": props.get("conc_oliv"),
            "ragweed_conc": props.get("conc_ambr"),
            "date": props.get("date_ech"),
            "source": props.get("source"),
            "geom": Point(lon, lat),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


def run(departements: list[str] | None = None):
    ensure_postgis()
    ensure_schema(SCHEMA)

    console.print("\n[bold]ATMO Pollen Index[/bold]")

    token = _get_token()
    console.print("  [green]Authenticated[/]")

    target = date.today().isoformat()
    console.print(f"  Fetching pollen indices for {target}...")

    gdf = _fetch_pollens(token, target)
    console.print(f"  -> {len(gdf):,} commune pollen indices fetched")

    if gdf.empty:
        console.print("[red]No data.[/red]")
        return

    qualified = f"{SCHEMA}.{TABLE}"
    console.print(f"\n[bold]Loading into {qualified}...[/bold]")

    gdf.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)

    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {SCHEMA}.{TABLE} USING GIST (geom)"
        ))
        conn.commit()

    console.print(f"[green]Done — {len(gdf):,} pollen records loaded into {qualified}[/green]")
