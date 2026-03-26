"""
OSM transport pipeline — Train stations, metro stations, and bus stops.

Schema: osm
Table: transport

Source: Overpass API
"""

import time

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import Point

from common import (
    ensure_schema,
    query_overpass,
)
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "osm"
TABLE = "transport"

# Overpass query: train stations, metro/tram stops, bus stops
OVERPASS_BODY = """  node["railway"="station"]({bbox});
  node["railway"="halt"]({bbox});
  node["station"="subway"]({bbox});
  node["railway"="tram_stop"]({bbox});
  node["highway"="bus_stop"]({bbox});
  node["amenity"="bus_station"]({bbox});"""


def parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue

        tags = el.get("tags", {})

        # Determine transport type
        transport_type = "bus_stop"
        if tags.get("railway") == "station" or tags.get("railway") == "halt":
            transport_type = "train_station"
        elif tags.get("station") == "subway":
            transport_type = "metro_station"
        elif tags.get("railway") == "tram_stop":
            transport_type = "tram_stop"
        elif tags.get("amenity") == "bus_station":
            transport_type = "bus_station"

        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "transport_type": transport_type,
            "network": tags.get("network"),
            "operator": tags.get("operator"),
            "line": tags.get("line"),
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
    all_frames = []

    for i, dep in enumerate(departements):
        if i > 0:
            time.sleep(5)
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")
        try:
            elements = query_overpass(dep, OVERPASS_BODY)
            console.print(f"  -> {len(elements)} OSM elements found")

            if not elements:
                continue

            gdf = parse_elements(elements, dep)
            console.print(f"  -> {len(gdf)} transport stops parsed")
            all_frames.append(gdf)
        except Exception as e:
            console.print(f"  [red]Skipping {dep}: {e}[/]")

    if not all_frames:
        console.print("[red]No data to load.[/red]")
        return

    final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True))
    final = final.set_crs(epsg=4326)
    final = final.rename_geometry("geom")

    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        conn.execute(sa_text(f"DROP TABLE IF EXISTS {qualified} CASCADE"))
        conn.commit()

    console.print(f"\n[bold]Loading into {qualified}...[/bold]")
    final.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="replace", index=False)

    with engine.connect() as conn:
        conn.execute(sa_text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {qualified} USING GIST (geom)"))
        conn.commit()

    console.print(f"[green]Done — {len(final)} transport stops loaded into {qualified}[/green]")
