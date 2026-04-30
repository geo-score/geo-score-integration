"""
OSM nightclubs pipeline — Nightlife and noise-generating venues.

Schema: osm
Table: nightclubs

Captures venues that typically generate evening/night noise nuisance:
nightclubs, bars, pubs, music venues, gambling, beer gardens, stripclubs.

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
    query_overpass,
)
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "osm"
TABLE = "nightclubs"

AMENITY_TAGS = [
    "nightclub", "bar", "pub", "biergarten",
    "stripclub", "swingerclub",
    "casino", "gambling", "music_venue",
]

LEISURE_TAGS = [
    "adult_gaming_centre",
]

OVERPASS_BODY = """  node["amenity"~"^({amenities})$"]({{bbox}});
  node["leisure"~"^({leisure})$"]({{bbox}});"""


def _build_query_body() -> str:
    return OVERPASS_BODY.format(
        amenities="|".join(AMENITY_TAGS),
        leisure="|".join(LEISURE_TAGS),
    )


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
            "amenity": tags.get("amenity"),
            "leisure": tags.get("leisure"),
            "opening_hours": tags.get("opening_hours"),
            "addr_street": tags.get("addr:street"),
            "addr_housenumber": tags.get("addr:housenumber"),
            "addr_postcode": tags.get("addr:postcode"),
            "addr_city": tags.get("addr:city"),
            "departement": dep,
            "geom": Point(lon, lat),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geom", crs="EPSG:4326")


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
        try:
            elements = query_overpass(dep, query_body)
            console.print(f"  -> {len(elements)} OSM elements found")

            if not elements:
                continue

            gdf = parse_elements(elements, dep)
            console.print(f"  -> {len(gdf)} venues parsed")
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

    console.print(f"[green]Done — {len(final)} venues loaded into {qualified}[/green]")
