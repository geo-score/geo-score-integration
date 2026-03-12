"""
OSM shops pipeline — Shop and amenity points from OpenStreetMap.

Schema: osm
Table: shops

Source: Overpass API
"""

import time

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import Point

from common import (
    delete_existing_departments,
    ensure_schema,
    load_geodataframe,
    query_overpass,
)
from settings.db import ensure_postgis

console = Console()

SCHEMA = "osm"
TABLE = "shops"

AMENITY_TAGS = [
    "restaurant", "cafe", "bar", "pub", "fast_food", "bakery",
    "pharmacy", "bank", "post_office", "fuel",
    "dentist", "doctors", "veterinary",
    "cinema", "theatre", "library",
]

OVERPASS_BODY = """  node["shop"]({{bbox}});
  node["amenity"~"^({amenities})$"]({{bbox}});"""


def _build_query_body() -> str:
    amenity_filter = "|".join(AMENITY_TAGS)
    return OVERPASS_BODY.format(amenities=amenity_filter)


def parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue

        tags = el.get("tags", {})
        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "shop": tags.get("shop"),
            "amenity": tags.get("amenity"),
            "cuisine": tags.get("cuisine"),
            "brand": tags.get("brand"),
            "opening_hours": tags.get("opening_hours"),
            "addr_street": tags.get("addr:street"),
            "addr_housenumber": tags.get("addr:housenumber"),
            "addr_postcode": tags.get("addr:postcode"),
            "addr_city": tags.get("addr:city"),
            "departement": dep,
            "geometry": Point(lon, lat),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    qualified = f"{SCHEMA}.{TABLE}"
    query_body = _build_query_body()
    all_frames = []

    for i, dep in enumerate(departements):
        if i > 0:
            time.sleep(5)
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")

        elements = query_overpass(dep, query_body)
        console.print(f"  -> {len(elements)} OSM elements found")

        if not elements:
            continue

        gdf = parse_elements(elements, dep)
        console.print(f"  -> {len(gdf)} points parsed")
        all_frames.append(gdf)

    if not all_frames:
        console.print("[red]No data to load.[/red]")
        return

    final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True))
    final = final.set_crs(epsg=4326)

    delete_existing_departments(qualified, departements)

    console.print(f"\n[bold]Loading into {qualified}...[/bold]")
    load_geodataframe(final, TABLE, SCHEMA, geom_type="Point")

    console.print(f"[green]Done — {len(final)} points loaded into {qualified}[/green]")
