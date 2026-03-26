"""
OSM roads pipeline — Road network and parking near a location.

Schema: osm
Table: roads, parking

Source: Overpass API
"""

import time

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import LineString, Point, Polygon

from common import (
    delete_existing_departments,
    ensure_schema,
    query_overpass,
)
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "osm"

# Roads: major roads that generate traffic noise
ROADS_TABLE = "roads"
ROADS_BODY = """  way["highway"~"^(motorway|trunk|primary|secondary|tertiary|motorway_link|trunk_link|primary_link)$"]({bbox});"""

# Parking
PARKING_TABLE = "parking"
PARKING_BODY = """  node["amenity"="parking"]({bbox});
  way["amenity"="parking"]({bbox});
  node["amenity"="parking_entrance"]({bbox});"""


def _parse_road_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    nodes = {}
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = (el["lon"], el["lat"])

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
            "highway": tags.get("highway"),
            "lanes": tags.get("lanes"),
            "maxspeed": tags.get("maxspeed"),
            "surface": tags.get("surface"),
            "departement": dep,
            "geom": LineString(coords),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


def _parse_parking_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        tags = el.get("tags", {})

        if el["type"] == "node":
            lat, lon = el.get("lat"), el.get("lon")
            if lat is None or lon is None:
                continue
            geom = Point(lon, lat)
        elif el["type"] == "way" and "geometry" in el:
            coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
            if len(coords) < 4 or coords[0] != coords[-1]:
                continue
            geom = Polygon(coords)
        else:
            continue

        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "parking_type": tags.get("parking"),
            "capacity": tags.get("capacity"),
            "fee": tags.get("fee"),
            "access": tags.get("access"),
            "departement": dep,
            "geom": geom,
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
            f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {SCHEMA}.{table} USING GIST (geom)"
        ))
        conn.commit()


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    road_frames = []
    parking_frames = []

    for i, dep in enumerate(departements):
        if i > 0:
            time.sleep(5)
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")

        # Roads
        try:
            elements = query_overpass(dep, ROADS_BODY, out_mode="body geom")
            roads = _parse_road_elements(elements, dep)
            console.print(f"  -> {len(roads)} road segments")
            if not roads.empty:
                road_frames.append(roads)
        except Exception as e:
            console.print(f"  [red]Roads skipped: {e}[/]")

        time.sleep(3)

        # Parking
        try:
            elements = query_overpass(dep, PARKING_BODY, out_mode="body geom")
            parking = _parse_parking_elements(elements, dep)
            console.print(f"  -> {len(parking)} parking locations")
            if not parking.empty:
                parking_frames.append(parking)
        except Exception as e:
            console.print(f"  [red]Parking skipped: {e}[/]")

    _load_table(road_frames, ROADS_TABLE, departements)
    _load_table(parking_frames, PARKING_TABLE, departements)

    total = sum(len(f) for f in road_frames) + sum(len(f) for f in parking_frames)
    console.print(f"\n[green]Done — {total:,} road/parking features loaded into {SCHEMA}.*[/green]")
