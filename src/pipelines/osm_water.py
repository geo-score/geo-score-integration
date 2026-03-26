"""
OSM water bodies pipeline — Rivers, lakes, canals.

Schema: osm
Table: water

Source: Overpass API
"""

import time

import geopandas as gpd
import pandas as pd
from rich.console import Console
from shapely.geometry import LineString, Polygon

from common import (
    delete_existing_departments,
    ensure_schema,
    query_overpass,
)
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "osm"
TABLE = "water"

OVERPASS_BODY = """  way["natural"="water"]({bbox});
  relation["natural"="water"]({bbox});
  way["waterway"~"^(river|canal|stream)$"]({bbox});"""


def parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        tags = el.get("tags", {})
        geom = None

        if el["type"] == "way" and "geometry" in el:
            coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
            if len(coords) >= 4 and coords[0] == coords[-1]:
                geom = Polygon(coords)
            elif len(coords) >= 2:
                geom = LineString(coords)

        elif el["type"] == "relation" and "members" in el:
            outer_rings = []
            for member in el["members"]:
                if member.get("role") == "outer" and "geometry" in member:
                    coords = [(p["lon"], p["lat"]) for p in member["geometry"]]
                    if len(coords) >= 4 and coords[0] == coords[-1]:
                        outer_rings.append(Polygon(coords))
            if outer_rings:
                geom = outer_rings[0] if len(outer_rings) == 1 else outer_rings[0]

        if geom is None or geom.is_empty:
            continue

        water_type = tags.get("waterway") or tags.get("water") or "water"

        rows.append({
            "osm_id": el["id"],
            "name": tags.get("name"),
            "water_type": water_type,
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

            gdf = parse_elements(elements, dep)
            console.print(f"  -> {len(gdf)} water features parsed")
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
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom ON {SCHEMA}.{TABLE} USING GIST (geom)"))
        conn.commit()

    console.print(f"[green]Done — {len(final):,} water features loaded into {qualified}[/green]")
