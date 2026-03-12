"""
OSM commerce pipeline — Shop and amenity points from OpenStreetMap.

Schema: osm_shops
Tables: one per date snapshot (e.g. osm_shops.d2026_03_12)

Source: Overpass API (https://overpass-api.de/api/interpreter)
"""

import time
from datetime import date

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from shapely.geometry import Point

from integration.common import (
    delete_existing_departments,
    ensure_schema,
    load_geodataframe,
)
from integration.db import ensure_postgis

console = Console()

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

SCHEMA = "osm_shops"

AMENITY_TAGS = [
    "restaurant", "cafe", "bar", "pub", "fast_food", "bakery",
    "pharmacy", "bank", "post_office", "fuel",
    "dentist", "doctors", "veterinary",
    "cinema", "theatre", "library",
]

# Department bounding boxes (approximate) — (south, west, north, east)
DEP_BBOX: dict[str, tuple[float, float, float, float]] = {
    "01": (45.6, 4.7, 46.5, 6.2),
    "02": (49.0, 3.0, 49.9, 4.1),
    "03": (46.1, 2.3, 46.8, 3.7),
    "04": (43.7, 5.6, 44.7, 6.9),
    "05": (44.2, 5.4, 45.1, 7.1),
    "06": (43.5, 6.6, 44.4, 7.7),
    "07": (44.3, 3.9, 45.4, 4.9),
    "08": (49.2, 4.0, 50.2, 5.4),
    "09": (42.6, 0.8, 43.3, 2.2),
    "10": (47.9, 3.4, 48.7, 4.5),
    "11": (42.6, 1.7, 43.5, 3.2),
    "12": (43.7, 1.8, 44.9, 3.5),
    "13": (43.2, 4.2, 43.9, 5.8),
    "14": (48.7, -0.9, 49.4, 0.4),
    "15": (44.6, 2.1, 45.5, 3.4),
    "16": (45.2, -0.5, 46.1, 0.7),
    "17": (45.1, -1.3, 46.4, 0.0),
    "18": (46.4, 1.8, 47.6, 3.1),
    "19": (45.0, 1.2, 45.8, 2.5),
    "21": (46.9, 4.1, 47.9, 5.5),
    "22": (48.2, -3.6, 48.9, -1.9),
    "23": (45.6, 1.4, 46.4, 2.6),
    "24": (44.6, 0.0, 45.7, 1.5),
    "25": (46.6, 5.7, 47.6, 7.1),
    "26": (44.1, 4.6, 45.3, 5.8),
    "27": (48.7, 0.3, 49.5, 1.8),
    "28": (47.9, 0.8, 48.6, 1.9),
    "29": (47.7, -5.2, 48.8, -3.4),
    "2A": (41.4, 8.6, 42.0, 9.4),
    "2B": (42.0, 9.0, 43.0, 9.6),
    "30": (43.5, 3.3, 44.5, 4.8),
    "31": (42.7, 0.4, 43.9, 2.0),
    "32": (43.3, -0.3, 44.1, 1.2),
    "33": (44.2, -1.3, 45.6, 0.3),
    "34": (43.2, 2.5, 43.9, 4.2),
    "35": (47.7, -2.3, 48.6, -1.0),
    "36": (46.3, 0.9, 47.2, 2.2),
    "37": (46.7, 0.1, 47.7, 1.4),
    "38": (44.7, 5.1, 45.9, 6.4),
    "39": (46.3, 5.3, 47.0, 6.2),
    "40": (43.5, -1.5, 44.5, 0.2),
    "41": (47.2, 0.6, 48.1, 2.2),
    "42": (45.2, 3.7, 46.3, 4.8),
    "43": (44.7, 3.1, 45.4, 4.5),
    "44": (46.9, -2.6, 47.6, -1.0),
    "45": (47.5, 1.5, 48.3, 3.1),
    "46": (44.2, 1.0, 45.0, 2.2),
    "47": (43.8, 0.0, 44.8, 1.1),
    "48": (44.1, 2.9, 44.9, 3.9),
    "49": (47.1, -1.4, 47.8, 0.2),
    "50": (48.5, -1.9, 49.7, -0.8),
    "51": (48.5, 3.4, 49.4, 4.9),
    "52": (47.6, 4.6, 48.4, 5.9),
    "53": (47.7, -1.2, 48.4, -0.1),
    "54": (48.3, 5.4, 49.1, 7.1),
    "55": (48.4, 4.9, 49.4, 5.9),
    "56": (47.3, -3.7, 48.2, -2.0),
    "57": (48.5, 5.9, 49.5, 7.6),
    "58": (46.7, 2.8, 47.6, 4.2),
    "59": (50.0, 2.1, 51.1, 4.2),
    "60": (49.1, 1.7, 49.8, 3.2),
    "61": (48.2, -0.2, 48.8, 1.0),
    "62": (50.0, 1.6, 50.9, 3.2),
    "63": (45.3, 2.4, 46.3, 3.9),
    "64": (43.0, -1.8, 43.6, -0.1),
    "65": (42.7, -0.3, 43.4, 0.7),
    "66": (42.3, 1.7, 42.9, 3.2),
    "67": (48.1, 7.0, 49.1, 8.2),
    "68": (47.4, 6.8, 48.3, 7.6),
    "69": (45.5, 4.2, 46.3, 5.0),
    "70": (47.3, 5.6, 48.0, 6.8),
    "71": (46.2, 3.6, 47.2, 5.0),
    "72": (47.6, -0.5, 48.5, 1.0),
    "73": (45.1, 5.6, 45.9, 7.2),
    "74": (45.7, 5.8, 46.4, 7.0),
    "75": (48.815, 2.22, 48.905, 2.47),
    "76": (49.2, 0.1, 50.1, 1.8),
    "77": (48.1, 2.4, 49.1, 3.6),
    "78": (48.4, 1.4, 49.1, 2.2),
    "79": (46.1, -0.9, 47.1, 0.2),
    "80": (49.6, 1.4, 50.4, 3.2),
    "81": (43.4, 1.5, 44.2, 2.9),
    "82": (43.8, 0.7, 44.4, 1.9),
    "83": (43.0, 5.7, 43.8, 6.9),
    "84": (43.7, 4.6, 44.4, 5.8),
    "85": (46.3, -2.4, 47.1, -0.5),
    "86": (46.1, -0.1, 47.2, 1.2),
    "87": (45.4, 0.6, 46.4, 1.9),
    "88": (47.8, 5.4, 48.5, 7.2),
    "89": (47.3, 2.8, 48.4, 4.3),
    "90": (47.4, 6.7, 47.8, 7.2),
    "91": (48.3, 1.9, 48.8, 2.6),
    "92": (48.73, 2.15, 48.95, 2.34),
    "93": (48.84, 2.28, 49.01, 2.60),
    "94": (48.68, 2.30, 48.86, 2.62),
    "95": (48.9, 1.6, 49.3, 2.6),
    "971": (15.8, -61.8, 16.5, -61.0),
    "972": (14.4, -61.3, 14.9, -60.8),
    "973": (2.1, -54.6, 5.8, -51.6),
    "974": (-21.4, 55.2, -20.9, 55.8),
    "976": (-13.0, 45.0, -12.6, 45.3),
}


def _build_overpass_query(bbox: tuple[float, float, float, float]) -> str:
    s, w, n, e = bbox
    bbox_str = f"{s},{w},{n},{e}"
    amenity_filter = "|".join(AMENITY_TAGS)
    return f"""
[out:json][timeout:180];
(
  node["shop"]({bbox_str});
  node["amenity"~"^({amenity_filter})$"]({bbox_str});
);
out center;
"""


def query_overpass(dep: str, max_retries: int = 5) -> list[dict]:
    """Query Overpass API with retry on 429."""
    bbox = DEP_BBOX.get(dep)
    if not bbox:
        console.print(f"  [yellow]No bounding box for department {dep}, skipping[/yellow]")
        return []

    query = _build_overpass_query(bbox)
    console.print(f"  Querying Overpass API for {dep}...")

    for attempt in range(max_retries):
        resp = httpx.post(OVERPASS_URL, data={"data": query}, timeout=200)
        if resp.status_code == 429:
            wait = 15 * (attempt + 1)
            console.print(f"  [yellow]Rate limited, waiting {wait}s...[/yellow]")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json().get("elements", [])

    console.print(f"  [red]Failed after {max_retries} retries for {dep}[/red]")
    return []


def parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    """Parse Overpass elements into a GeoDataFrame of points."""
    rows = []
    for el in elements:
        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            continue

        tags = el.get("tags", {})
        rows.append({
            "osm_id": el["id"],
            "osm_type": el["type"],
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


def run(departements: list[str], snapshot: date | None = None):
    """Main pipeline: query Overpass, parse, and load into DB."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    if snapshot is None:
        snapshot = date.today()

    table = f"d{snapshot.strftime('%Y_%m_%d')}"
    qualified = f"{SCHEMA}.{table}"

    all_frames = []

    for i, dep in enumerate(departements):
        if i > 0:
            time.sleep(5)
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")

        elements = query_overpass(dep)
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
    load_geodataframe(final, table, SCHEMA, geom_type="Point")

    console.print(f"[green]Done — {len(final)} points loaded into {qualified}[/green]")
