"""
DPE Collectif pipeline — Building-level energy performance from ADEME.

Schema: energy
Table: dpe_collectif

Source: ADEME Open Data API
        https://data.ademe.fr/data-fair/api/v1/datasets/meg-83tjwtg8dyz4vv7h1dqe

Filters to "immeuble" type DPE and loads geocoded records with energy
performance, heating type, insulation quality, and cost estimates.
"""

import time

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from shapely.geometry import Point

from common import delete_existing_departments, ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "energy"
TABLE = "dpe_collectif"

DATASET_ID = "meg-83tjwtg8dyz4vv7h1dqe"
API_BASE = f"https://data.ademe.fr/data-fair/api/v1/datasets/{DATASET_ID}/lines"

SELECT_FIELDS = ",".join([
    "numero_dpe",
    "type_batiment",
    "etiquette_dpe",
    "etiquette_ges",
    "conso_5_usages_par_m2_ep",
    "emission_ges_5_usages_par_m2",
    "type_energie_principale_chauffage",
    "type_energie_principale_ecs",
    "type_installation_chauffage",
    "type_installation_ecs",
    "qualite_isolation_enveloppe",
    "qualite_isolation_murs",
    "qualite_isolation_menuiseries",
    "qualite_isolation_plancher_bas",
    "qualite_isolation_plancher_haut_comble_perdu",
    "surface_habitable_immeuble",
    "nombre_niveau_immeuble",
    "nombre_appartement",
    "periode_construction",
    "cout_total_5_usages",
    "cout_chauffage",
    "cout_ecs",
    "date_etablissement_dpe",
    "code_postal_ban",
    "nom_commune_ban",
    "code_departement_ban",
    "code_insee_ban",
    "_geopoint",
])

PAGE_SIZE = 5_000
MAX_PAGES = 200


def _fetch_department(dep: str) -> pd.DataFrame:
    """Fetch all DPE collectifs for a department via paginated API calls."""
    all_rows = []
    after = None

    for page in range(MAX_PAGES):
        params = {
            "size": PAGE_SIZE,
            "select": SELECT_FIELDS,
            "qs": f'code_departement_ban:"{dep}" AND type_batiment:"immeuble"',
        }
        if after:
            params["after"] = after

        try:
            resp = httpx.get(API_BASE, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            console.print(f"  [red]API error page {page}: {e}[/]")
            break

        results = data.get("results", [])
        if not results:
            break

        all_rows.extend(results)

        next_url = data.get("next")
        if not next_url:
            break

        # Extract 'after' param from next URL
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(next_url)
        after = parse_qs(parsed.query).get("after", [None])[0]

        if page % 10 == 9:
            console.print(f"  ... {len(all_rows):,} records fetched")
            time.sleep(0.5)

    return pd.DataFrame(all_rows)


def _to_geodataframe(df: pd.DataFrame, dep: str) -> gpd.GeoDataFrame:
    """Convert API results to GeoDataFrame."""
    # Filter rows with valid geopoints
    valid = df["_geopoint"].notna() & (df["_geopoint"] != "")
    df = df[valid].copy()

    if df.empty:
        return gpd.GeoDataFrame()

    # Parse geopoints (lat,lon format)
    coords = df["_geopoint"].str.split(",", expand=True)
    df["lat"] = pd.to_numeric(coords[0], errors="coerce")
    df["lon"] = pd.to_numeric(coords[1], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])

    geometry = [Point(lon, lat) for lon, lat in zip(df["lon"], df["lat"])]
    gdf = gpd.GeoDataFrame(
        df.drop(columns=["_geopoint", "lat", "lon"]),
        geometry=geometry,
        crs="EPSG:4326",
    )
    gdf = gdf.rename_geometry("geom")
    gdf["departement"] = dep

    return gdf


def run(departements: list[str]):
    ensure_postgis()
    ensure_schema(SCHEMA)

    qualified = f"{SCHEMA}.{TABLE}"
    all_frames = []

    for i, dep in enumerate(departements):
        console.print(f"\n[bold]Department {dep} ({i + 1}/{len(departements)})[/bold]")
        try:
            df = _fetch_department(dep)
            console.print(f"  -> {len(df):,} DPE records fetched")

            if df.empty:
                continue

            gdf = _to_geodataframe(df, dep)
            console.print(f"  -> {len(gdf):,} geocoded records")

            if not gdf.empty:
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
    final.to_postgis(TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_geom "
            f"ON {SCHEMA}.{TABLE} USING GIST (geom)"
        ))
        conn.commit()

    console.print(f"[green]Done — {len(final):,} DPE collectifs loaded into {qualified}[/green]")
