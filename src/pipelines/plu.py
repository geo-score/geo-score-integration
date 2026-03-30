"""
PLU — Plan Local d'Urbanisme (zonages + prescriptions)

Source : Géoportail de l'Urbanisme (GPU) — WFS 2.0.0
URL    : https://data.geopf.fr/wfs/ows
Schema : plu
Tables : zones, prescriptions

Strategy: fetch per commune (small fast requests) → save locally → merge → clean → load.

Zonage types:
  U   = Urbaine (constructible)
  AU  = À Urbaniser (AUc = court terme, AUs = strict)
  A   = Agricole (non constructible sauf dérogation)
  N   = Naturelle (non constructible)

Prescriptions: servitudes, emplacements réservés, EBC, patrimoine, etc.
"""

import io
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from sqlalchemy import text

from common import ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "plu"
WFS_URL = "https://data.geopf.fr/wfs/ows"
GEO_API_URL = "https://geo.api.gouv.fr/departements/{dep}/communes?fields=code&format=json"
PAGE_SIZE = 5000
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _get_communes(dep: str) -> list[str]:
    """Get list of commune codes for a department from geo.api.gouv.fr."""
    url = GEO_API_URL.format(dep=dep)
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    return [c["code"] for c in resp.json()]


def _fetch_page(url: str) -> bytes | None:
    """Download a single WFS page with timeout and retry. Returns raw bytes."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = httpx.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = 5 * attempt
                console.print(
                    f"    [yellow]Attempt {attempt}/{MAX_RETRIES} failed: {e} — retry in {wait}s[/]"
                )
                time.sleep(wait)
            else:
                console.print(f"    [red]Failed after {MAX_RETRIES} attempts: {e}[/]")
                return None


def _download_commune_layer(
    layer: str, commune: str, out_dir: Path, prefix: str
) -> int:
    """Download all pages of a WFS layer for one commune to local files.
    Returns number of features downloaded."""
    start_index = 0
    total = 0
    cql = f"partition LIKE 'DU_{commune}%'"

    while True:
        url = (
            f"{WFS_URL}?service=WFS&version=2.0.0&request=GetFeature"
            f"&typeName={layer}"
            f"&outputFormat=application/json"
            f"&count={PAGE_SIZE}"
            f"&startIndex={start_index}"
            f"&sortBy=gid"
            f"&CQL_FILTER={quote(cql)}"
        )

        data = _fetch_page(url)
        if data is None:
            break

        # Check if any features returned
        gdf = gpd.read_file(io.BytesIO(data))
        if gdf.empty:
            break

        # Save to local file
        dest = out_dir / f"{prefix}_{commune}_{start_index}.geojson"
        dest.write_bytes(data)
        total += len(gdf)

        if len(gdf) < PAGE_SIZE:
            break
        start_index += PAGE_SIZE

    return total


# ---------------------------------------------------------------------------
# Harmonisation
# ---------------------------------------------------------------------------

# GPU typezone → normalized category + English label
# typezone is standardized by CNIG but communes use varied libelle (Ua, Ub, UV, ...)
ZONE_CATEGORIES = {
    "U":   ("urban",            "Urban — buildable"),
    "AUc": ("to_urbanize",      "To urbanize — short term"),
    "AUs": ("to_urbanize_strict","To urbanize — strict (requires modification)"),
    "AU":  ("to_urbanize",      "To urbanize"),
    "A":   ("agricultural",     "Agricultural — not buildable"),
    "N":   ("natural",          "Natural — not buildable"),
}


def _harmonize_typezone(typezone: str) -> str:
    """Map GPU typezone to a normalized English category."""
    if typezone in ZONE_CATEGORIES:
        return ZONE_CATEGORIES[typezone][0]
    # Fallback: strip digits/suffixes and try base type
    base = typezone.rstrip("0123456789csCShH").upper() if typezone else ""
    for key in ("U", "AU", "A", "N"):
        if base == key:
            return ZONE_CATEGORIES[key][0]
    return "other"


# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

def _clean_zones(gdf: gpd.GeoDataFrame, dep: str) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf

    gdf = gdf.to_crs(epsg=4326)

    result = gpd.GeoDataFrame(
        {
            "typezone": gdf["typezone"],
            "zone_category": gdf["typezone"].apply(_harmonize_typezone),
            "libelle": gdf["libelle"],
            "libelong": gdf["libelong"],
            "destdomi": gdf.get("destdomi"),
            "idurba": gdf["idurba"],
            "datappro": gdf.get("datappro"),
            "urlfic": gdf.get("urlfic"),
            "departement": dep,
        },
        geometry=gdf.geometry.values,
        crs="EPSG:4326",
    )
    return result.rename_geometry("geom")


def _clean_prescriptions(gdf: gpd.GeoDataFrame, dep: str) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf

    gdf = gdf.to_crs(epsg=4326)

    result = gpd.GeoDataFrame(
        {
            "typepsc": gdf["typepsc"],
            "stypepsc": gdf.get("stypepsc"),
            "libelle": gdf["libelle"],
            "txt": gdf.get("txt"),
            "departement": dep,
        },
        geometry=gdf.geometry.values,
        crs="EPSG:4326",
    )
    return result.rename_geometry("geom")


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def _load_to_postgis(gdf: gpd.GeoDataFrame, table: str):
    if gdf.empty:
        return

    qualified = f"{SCHEMA}.{table}"
    gdf.to_postgis(table, engine, schema=SCHEMA, if_exists="append", index=False)

    with engine.connect() as conn:
        conn.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_geom "
                f"ON {qualified} USING GIST (geom)"
            )
        )
        conn.commit()


def _delete_department(table: str, dep: str):
    qualified = f"{SCHEMA}.{table}"
    with engine.connect() as conn:
        exists = conn.execute(
            text(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                f"WHERE table_schema = '{SCHEMA}' AND table_name = '{table}')"
            )
        ).scalar()
        if exists:
            conn.execute(
                text(f"DELETE FROM {qualified} WHERE departement = :dep"),
                {"dep": dep},
            )
        conn.commit()


def _merge_local_files(directory: Path, prefix: str) -> gpd.GeoDataFrame:
    """Read and concatenate all local GeoJSON files matching a prefix."""
    files = sorted(directory.glob(f"{prefix}_*.geojson"))
    if not files:
        return gpd.GeoDataFrame()

    frames = [gpd.read_file(f) for f in files]
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(departements: list[str]):
    """Download PLU per commune → merge → clean → load into PostGIS."""
    ensure_postgis()
    ensure_schema(SCHEMA)

    for i, dep in enumerate(departements):
        console.print(
            f"\n[bold cyan]Department {dep} ({i + 1}/{len(departements)})[/]"
        )

        # Get commune list
        try:
            communes = _get_communes(dep)
        except Exception as e:
            console.print(f"  [red]Cannot fetch communes: {e}[/]")
            continue

        console.print(f"  {len(communes)} communes")

        with tempfile.TemporaryDirectory(prefix=f"plu-{dep}-") as tmpdir:
            tmp = Path(tmpdir)

            # --- 1. Download all communes locally ---
            total_zones = 0
            total_presc = 0
            for j, commune in enumerate(communes):
                z = _download_commune_layer("wfs_du:zone_urba", commune, tmp, "zones")
                p = _download_commune_layer("wfs_du:prescription_surf", commune, tmp, "presc")
                total_zones += z
                total_presc += p

                if (j + 1) % 50 == 0 or j == len(communes) - 1:
                    console.print(
                        f"  [{j + 1}/{len(communes)}] "
                        f"zones: {total_zones:,} | prescriptions: {total_presc:,}"
                    )

            # --- 2. Merge local files ---
            console.print("  Merging zones...")
            zones_raw = _merge_local_files(tmp, "zones")
            console.print("  Merging prescriptions...")
            presc_raw = _merge_local_files(tmp, "presc")

            # --- 3. Clean ---
            zones = _clean_zones(zones_raw, dep)
            presc = _clean_prescriptions(presc_raw, dep)

            # --- 4. Load to DB ---
            _delete_department("zones", dep)
            if not zones.empty:
                _load_to_postgis(zones, "zones")
                console.print(f"  [green]{len(zones):,} zones loaded[/]")
            else:
                console.print(f"  [yellow]No zones for {dep}[/]")

            _delete_department("prescriptions", dep)
            if not presc.empty:
                _load_to_postgis(presc, "prescriptions")
                console.print(f"  [green]{len(presc):,} prescriptions loaded[/]")
            else:
                console.print(f"  [yellow]No prescriptions for {dep}[/]")

    console.print(f"\n[bold green]PLU pipeline done[/]")
