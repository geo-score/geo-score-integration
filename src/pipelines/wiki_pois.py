"""
Wiki POI enrichment pipeline — descriptions, images, and context from Wikidata + Wikipedia.

Schema: osm
Table: poi_wiki

Source: Overpass API (POIs with wikidata tag) → Wikidata API → Wikipedia API
"""

import time

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from shapely.geometry import Point

from common import DEP_BBOX, ensure_schema
from settings.db import engine, ensure_postgis

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

console = Console()

SCHEMA = "osm"
TABLE = "poi_wiki"

# Separate Overpass queries per category to avoid timeout on dense departments
OVERPASS_QUERIES = [
    ("tourism", 'node["wikidata"]["tourism"~"^(museum|gallery|hotel|attraction|viewpoint)$"]({bbox})'),
    ("historic", 'node["wikidata"]["historic"~"^(monument|castle|memorial|ruins|archaeological_site|church)$"]({bbox})'),
    ("amenity", 'node["wikidata"]["amenity"~"^(restaurant|cafe|bar|theatre|cinema|museum|place_of_worship)$"]({bbox})'),
    ("leisure", 'node["wikidata"]["leisure"~"^(park|garden|sports_centre|stadium)$"]({bbox})'),
]

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIPEDIA_API = "https://fr.wikipedia.org/w/api.php"

BATCH_SIZE = 20
USER_AGENT = "GeoScoreBot/1.0 (https://github.com/geo-score; contact@geo-score.fr) Python/httpx"


def _fetch_wikidata_batch(qids: list[str], client: httpx.Client) -> dict:
    """Fetch labels, descriptions, images, and sitelinks for a batch of Wikidata entities."""
    resp = client.post(WIKIDATA_API, data={
        "action": "wbgetentities",
        "ids": "|".join(qids),
        "props": "descriptions|claims",
        "languages": "fr|en",
        "format": "json",
    })
    resp.raise_for_status()
    return resp.json().get("entities", {})


def _extract_image(claims: dict) -> str | None:
    """Extract main image filename from Wikidata claims (P18)."""
    p18 = claims.get("P18", [])
    if p18:
        value = p18[0].get("mainsnak", {}).get("datavalue", {}).get("value")
        if value:
            safe = value.replace(" ", "_")
            return f"https://commons.wikimedia.org/wiki/Special:FilePath/{safe}?width=400"
    return None


def _extract_description(entity: dict) -> str | None:
    """Extract fr description, fallback to en."""
    descriptions = entity.get("descriptions", {})
    if "fr" in descriptions:
        return descriptions["fr"].get("value")
    if "en" in descriptions:
        return descriptions["en"].get("value")
    return None



def parse_elements(elements: list[dict], dep: str) -> gpd.GeoDataFrame:
    rows = []
    for el in elements:
        lat = el.get("lat") or (el.get("center", {}).get("lat"))
        lon = el.get("lon") or (el.get("center", {}).get("lon"))
        if lat is None or lon is None:
            continue

        tags = el.get("tags", {})
        qid = tags.get("wikidata")
        if not qid:
            continue

        # Build a single "category" from the first matching tag
        category = (
            tags.get("tourism")
            or tags.get("amenity")
            or tags.get("historic")
            or tags.get("leisure")
        )

        rows.append({
            "osm_id": el["id"],
            "wikidata_id": qid,
            "name": tags.get("name"),
            "category": category,
            "departement": dep,
            "geometry": Point(lon, lat),
        })

    if not rows:
        return gpd.GeoDataFrame()

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


def enrich_with_wikidata(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Enrich POIs with Wikidata description (1-line) and image URL."""
    qids = gdf["wikidata_id"].dropna().unique().tolist()
    console.print(f"  Fetching Wikidata for {len(qids)} entities...")

    wiki_data: dict[str, dict] = {}

    with httpx.Client(timeout=30, headers={"User-Agent": USER_AGENT}) as client:
        for i in range(0, len(qids), BATCH_SIZE):
            batch = qids[i:i + BATCH_SIZE]
            entities = _fetch_wikidata_batch(batch, client)
            for qid, entity in entities.items():
                if "missing" in entity:
                    continue
                wiki_data[qid] = {
                    "description": _extract_description(entity),
                    "image_url": _extract_image(entity.get("claims", {})),
                }
            time.sleep(0.2)

    gdf["description"] = gdf["wikidata_id"].map(lambda q: wiki_data.get(q, {}).get("description"))
    gdf["image_url"] = gdf["wikidata_id"].map(lambda q: wiki_data.get(q, {}).get("image_url"))

    return gdf


def _query_overpass_single(query_body: str, bbox_str: str) -> list[dict]:
    """Run a single Overpass query with GET to avoid POST encoding issues."""
    query = f"[out:json][timeout:120];{query_body.format(bbox=bbox_str)};out;"
    for attempt in range(3):
        resp = httpx.get(OVERPASS_URL, params={"data": query}, timeout=180)
        if resp.status_code == 429:
            wait = 15 * (attempt + 1)
            console.print(f"    [yellow]Rate limited, waiting {wait}s...[/yellow]")
            time.sleep(wait)
            continue
        if resp.status_code == 504:
            console.print(f"    [yellow]Timeout, retrying...[/yellow]")
            time.sleep(10)
            continue
        resp.raise_for_status()
        return resp.json().get("elements", [])
    return []


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    qualified = f"{SCHEMA}.{TABLE}"
    all_frames = []

    for i, dep in enumerate(departements):
        if i > 0:
            time.sleep(5)
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")

        bbox = DEP_BBOX.get(dep)
        if not bbox:
            console.print(f"  [yellow]No bbox for {dep}, skipping[/yellow]")
            continue

        s, w, n, e = bbox
        bbox_str = f"{s},{w},{n},{e}"

        # Query each category separately to avoid timeout
        elements = []
        seen_ids = set()
        for label, query_body in OVERPASS_QUERIES:
            try:
                time.sleep(2)
                results = _query_overpass_single(query_body, bbox_str)
                new = [el for el in results if el["id"] not in seen_ids]
                seen_ids.update(el["id"] for el in new)
                elements.extend(new)
                console.print(f"  {label}: {len(results)} found ({len(new)} new)")
            except Exception as exc:
                console.print(f"  [red]{label}: {exc}[/]")

        if not elements:
            continue

        try:
            gdf = parse_elements(elements, dep)
            if gdf.empty:
                continue
            console.print(f"  -> {len(gdf)} points parsed")

            gdf = enrich_with_wikidata(gdf)
            enriched = gdf[gdf["description"].notna() | gdf["image_url"].notna()]
            console.print(f"  -> {len(enriched)} enriched with description or image")

            all_frames.append(enriched)
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

    console.print(f"[green]Done — {len(final)} POIs loaded into {qualified}[/green]")
