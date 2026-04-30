"""
OSM industry pipeline — Industrial zones and works.

Schema: osm
Table: industry

Polygon zones tagged as industrial land use, factories, refineries, quarries,
warehouses. Used for proximity-based pollution/noise nuisance scoring.

Source: Overpass API
"""

import time

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import Polygon

from common import (
    delete_existing_departments,
    ensure_schema,
    query_overpass,
)
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "osm"
TABLE = "industry"

OVERPASS_BODY = """  way["landuse"="industrial"]({bbox});
  way["landuse"="quarry"]({bbox});
  way["landuse"="port"]({bbox});
  way["man_made"~"^(works|wastewater_plant|water_works|gasometer|chimney|petroleum_well|silo|storage_tank|pumping_station)$"]({bbox});
  way["industrial"]({bbox});
  way["building"~"^(industrial|warehouse|factory)$"]({bbox});"""


def _parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        if el["type"] != "way" or "geometry" not in el:
            continue

        coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
        if len(coords) < 4 or coords[0] != coords[-1]:
            continue

        tags = el.get("tags", {})
        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "landuse": tags.get("landuse"),
            "industrial": tags.get("industrial"),
            "man_made": tags.get("man_made"),
            "building": tags.get("building"),
            "operator": tags.get("operator"),
            "departement": dep,
            "geom": Polygon(coords),
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
            console.print(f"  -> {len(gdf)} industrial polygons parsed")
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

    console.print(f"[green]Done — {len(final)} industrial features loaded into {qualified}[/green]")
