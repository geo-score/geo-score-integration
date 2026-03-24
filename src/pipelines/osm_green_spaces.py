"""
OSM green spaces pipeline — Parks, gardens, playgrounds as polygons.

Schema: osm
Table: green_spaces

Source: Overpass API
"""

import time

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import Polygon, MultiPolygon

from common import (
    delete_existing_departments,
    ensure_schema,
    load_geodataframe,
    query_overpass,
)
from settings.db import ensure_postgis

console = Console()

SCHEMA = "osm"
TABLE = "green_spaces"

OVERPASS_BODY = """  way["leisure"~"^(park|garden|playground|dog_park|nature_reserve)$"]({bbox});
  relation["leisure"~"^(park|garden|playground|dog_park|nature_reserve)$"]({bbox});
  way["landuse"~"^(recreation_ground|village_green|forest|meadow)$"]({bbox});
  relation["landuse"~"^(recreation_ground|village_green|forest|meadow)$"]({bbox});"""


def parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    """Parse Overpass way/relation elements into polygons."""
    # Index nodes by id for coordinate lookup
    nodes = {}
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = (el["lon"], el["lat"])

    # Index ways by id for relation member lookup
    ways = {}
    for el in elements:
        if el["type"] == "way" and "geometry" in el:
            coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
            ways[el["id"]] = coords

    rows = []
    for el in elements:
        tags = el.get("tags", {})
        geom = None

        if el["type"] == "way" and "geometry" in el:
            coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
            if len(coords) >= 4 and coords[0] == coords[-1]:
                geom = Polygon(coords)

        elif el["type"] == "relation" and "members" in el:
            # Build multipolygon from outer way members
            outer_rings = []
            for member in el["members"]:
                if member.get("role") == "outer" and member.get("type") == "way":
                    if "geometry" in member:
                        coords = [(p["lon"], p["lat"]) for p in member["geometry"]]
                        if len(coords) >= 4 and coords[0] == coords[-1]:
                            outer_rings.append(Polygon(coords))
            if outer_rings:
                geom = MultiPolygon([(r.exterior.coords, []) for r in outer_rings])

        if geom is None or geom.is_empty:
            continue

        rows.append({
            "osm_id": el["id"],
            "osm_type": el["type"],
            "name": tags.get("name"),
            "leisure": tags.get("leisure"),
            "landuse": tags.get("landuse"),
            "access": tags.get("access"),
            "surface": tags.get("surface"),
            "departement": dep,
            "geometry": geom,
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


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

            gdf = parse_elements(elements, dep)
            console.print(f"  -> {len(gdf)} polygons parsed")
            all_frames.append(gdf)
        except Exception as e:
            console.print(f"  [red]Skipping {dep}: {e}[/]")

    if not all_frames:
        console.print("[red]No data to load.[/red]")
        return

    final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True))
    final = final.set_crs(epsg=4326)

    delete_existing_departments(qualified, departements)

    console.print(f"\n[bold]Loading into {qualified}...[/bold]")
    load_geodataframe(final, TABLE, SCHEMA)

    console.print(f"[green]Done — {len(final)} green spaces loaded into {qualified}[/green]")
