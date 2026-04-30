"""
OSM railways pipeline — Rail lines and stations.

Schema: osm
Tables:
  - railways         (LineString) — heavy rail, tram, subway, light rail
  - railway_stations (Point)      — stations, halts, tram stops, subway entrances

Source: Overpass API
"""

import time

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import LineString, Point

from common import (
    delete_existing_departments,
    ensure_schema,
    query_overpass,
)
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "osm"
LINES_TABLE = "railways"
STATIONS_TABLE = "railway_stations"

# Railway lines that generate noise/vibration
LINES_BODY = """  way["railway"~"^(rail|tram|subway|light_rail|narrow_gauge|monorail)$"]({bbox});"""

# Stations and stops
STATIONS_BODY = """  node["railway"~"^(station|halt|tram_stop|subway_entrance)$"]({bbox});
  node["public_transport"="station"]({bbox});"""


def _parse_line_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        if el["type"] != "way" or "geometry" not in el:
            continue

        tags = el.get("tags", {})
        coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
        if len(coords) < 2:
            continue

        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "railway": tags.get("railway"),
            "operator": tags.get("operator"),
            "service": tags.get("service"),
            "usage": tags.get("usage"),
            "electrified": tags.get("electrified"),
            "maxspeed": tags.get("maxspeed"),
            "tunnel": tags.get("tunnel"),
            "bridge": tags.get("bridge"),
            "departement": dep,
            "geom": LineString(coords),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


def _parse_station_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        if el["type"] != "node":
            continue
        lat, lon = el.get("lat"), el.get("lon")
        if lat is None or lon is None:
            continue

        tags = el.get("tags", {})
        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "railway": tags.get("railway"),
            "station": tags.get("station"),
            "network": tags.get("network"),
            "operator": tags.get("operator"),
            "departement": dep,
            "geom": Point(lon, lat),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


def _load_table(frames: list[gpd.GeoDataFrame], table: str, departements: list[str]):
    if not frames:
        return
    final = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True)).set_crs(epsg=4326)
    qualified = f"{SCHEMA}.{table}"
    delete_existing_departments(qualified, departements)
    console.print(f"  Loading {len(final):,} rows into {qualified}...")
    final.to_postgis(table, engine, schema=SCHEMA, if_exists="append", index=False)

    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {qualified} USING GIST (geom)"
        ))
        conn.commit()


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    line_frames = []
    station_frames = []

    for i, dep in enumerate(departements):
        if i > 0:
            time.sleep(5)
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")

        try:
            elements = query_overpass(dep, LINES_BODY, out_mode="body geom")
            lines = _parse_line_elements(elements, dep)
            console.print(f"  -> {len(lines)} railway segments")
            if not lines.empty:
                line_frames.append(lines)
        except Exception as e:
            console.print(f"  [red]Lines skipped: {e}[/]")

        time.sleep(3)

        try:
            elements = query_overpass(dep, STATIONS_BODY)
            stations = _parse_station_elements(elements, dep)
            console.print(f"  -> {len(stations)} stations/stops")
            if not stations.empty:
                station_frames.append(stations)
        except Exception as e:
            console.print(f"  [red]Stations skipped: {e}[/]")

    _load_table(line_frames, LINES_TABLE, departements)
    _load_table(station_frames, STATIONS_TABLE, departements)

    total = sum(len(f) for f in line_frames) + sum(len(f) for f in station_frames)
    console.print(f"\n[green]Done — {total:,} railway features loaded into {SCHEMA}.*[/green]")
