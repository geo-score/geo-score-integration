"""
OSM airports pipeline — Aerodromes, runways, helipads.

Schema: osm
Table: airports

Mixed geometry table (Polygon for aerodromes/runways/terminals, Point for helipads
or unnamed aerodrome nodes). Used for proximity-based noise nuisance scoring.

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
TABLE = "airports"

# aeroway can be node (helipad), way (runway/taxiway), or way/relation (aerodrome polygon)
OVERPASS_BODY = """  node["aeroway"~"^(aerodrome|helipad|heliport)$"]({bbox});
  way["aeroway"~"^(aerodrome|runway|taxiway|terminal|helipad|heliport|apron)$"]({bbox});"""


def _parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        tags = el.get("tags", {})
        aeroway = tags.get("aeroway")

        if el["type"] == "node":
            lat, lon = el.get("lat"), el.get("lon")
            if lat is None or lon is None:
                continue
            geom = Point(lon, lat)
        elif el["type"] == "way" and "geometry" in el:
            coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
            if len(coords) < 2:
                continue
            # Polygons (aerodrome, terminal, apron) typically close back to first point
            if len(coords) >= 4 and coords[0] == coords[-1]:
                geom = Polygon(coords)
            else:
                geom = LineString(coords)
        else:
            continue

        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "aeroway": aeroway,
            "iata": tags.get("iata"),
            "icao": tags.get("icao"),
            "operator": tags.get("operator"),
            "use": tags.get("aerodrome:type") or tags.get("aerodrome"),
            "departement": dep,
            "geom": geom,
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    qualified = f"{SCHEMA}.{TABLE}"
    all_frames = []

    for i, dep in enumerate(departements):
        if i > 0:
            time.sleep(5)
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")
        try:
            elements = query_overpass(dep, OVERPASS_BODY, out_mode="body geom")
            console.print(f"  -> {len(elements)} OSM elements found")

            if not elements:
                continue

            gdf = _parse_elements(elements, dep)
            console.print(f"  -> {len(gdf)} airport features parsed")
            if not gdf.empty:
                all_frames.append(gdf)
        except Exception as e:
            console.print(f"  [red]Skipping {dep}: {e}[/]")

    if not all_frames:
        console.print("[red]No data to load.[/red]")
        return

    final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True)).set_crs(epsg=4326)

    delete_existing_departments(qualified, departements)

    console.print(f"\n[bold]Loading into {qualified}...[/bold]")
    final.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {qualified} USING GIST (geom)"
        ))
        conn.commit()

    console.print(f"[green]Done — {len(final)} airport features loaded into {qualified}[/green]")
