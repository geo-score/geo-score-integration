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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, MofNCompleteColumn, TimeElapsedColumn
from sqlalchemy import text

from common import ensure_schema
from settings.db import engine, ensure_postgis

console = Console()

SCHEMA = "plu"
WFS_URL = "https://data.geopf.fr/wfs/ows"
GEO_API_URL = "https://geo.api.gouv.fr/departements/{dep}/communes?fields=code&format=json"
PAGE_SIZE = 5000
REQUEST_TIMEOUT = 15
MAX_WORKERS = 5  # parallel commune downloads


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _get_communes(dep: str) -> list[str]:
    """Get list of commune codes for a department from geo.api.gouv.fr."""
    url = GEO_API_URL.format(dep=dep)
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    return [c["code"] for c in resp.json()]


def _get_commune_doc_status(dep: str, communes: list[str]) -> dict[str, str | None]:
    """Query wfs_du:document to get PLU/PLUi/CC status per commune.

    Returns a dict mapping commune code → du_type (PLU, PLUi, CC) or None (RNU).
    """
    status: dict[str, str | None] = {c: None for c in communes}

    cql = f"partition LIKE 'DU_{dep}%'"
    start_index = 0

    while True:
        url = (
            f"{WFS_URL}?service=WFS&version=2.0.0&request=GetFeature"
            f"&typeName=wfs_du:document"
            f"&outputFormat=application/json"
            f"&count={PAGE_SIZE}"
            f"&startIndex={start_index}"
            f"&sortBy=gid"
            f"&CQL_FILTER={quote(cql)}"
        )
        data = _fetch_page(url)
        if data is None:
            break

        gdf = gpd.read_file(io.BytesIO(data))
        if gdf.empty:
            break

        for _, row in gdf.iterrows():
            code = row.get("grid_name")
            du_type = row.get("du_type")
            if code and code in status:
                status[code] = du_type

        if len(gdf) < PAGE_SIZE:
            break
        start_index += PAGE_SIZE

    return status


def _fetch_page(url: str) -> bytes | None:
    """Download a single WFS page. Server errors (5xx) → skip immediately."""
    try:
        resp = httpx.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        return resp.content
    except httpx.HTTPStatusError as e:
        if e.response.status_code >= 500:
            return None
        raise
    except Exception:
        return None


def _download_commune_layer(
    layer: str, commune: str, out_dir: Path, prefix: str,
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

        # Check document status per commune (PLU/PLUi/CC vs RNU)
        doc_status = _get_commune_doc_status(dep, communes)
        covered = {c for c, s in doc_status.items() if s is not None}
        rnu = {c for c, s in doc_status.items() if s is None}

        # Count by type
        type_counts: dict[str, int] = {}
        for s in doc_status.values():
            key = s or "RNU"
            type_counts[key] = type_counts.get(key, 0) + 1
        summary = ", ".join(f"{t}: {n}" for t, n in sorted(type_counts.items()))
        console.print(f"  Document status: {summary}")

        if not covered:
            console.print(f"  [yellow]All communes under RNU — skipping[/]")
            continue

        covered_list = [c for c in communes if c in covered]
        console.print(f"  Downloading {len(covered_list)} covered communes...")

        with tempfile.TemporaryDirectory(prefix=f"plu-{dep}-") as tmpdir:
            tmp = Path(tmpdir)

            # --- 1. Download covered communes in parallel ---
            total_zones = 0
            total_presc = 0

            def _download_commune(commune: str) -> tuple[int, int]:
                z = _download_commune_layer("wfs_du:zone_urba", commune, tmp, "zones")
                p = _download_commune_layer("wfs_du:prescription_surf", commune, tmp, "presc")
                return z, p

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("zones:{task.fields[zones]} presc:{task.fields[presc]}"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                dl_task = progress.add_task(
                    f"  Downloading {dep}",
                    total=len(covered_list),
                    zones=0,
                    presc=0,
                )
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {
                        pool.submit(_download_commune, c): c
                        for c in covered_list
                    }
                    for future in as_completed(futures):
                        z, p = future.result()
                        total_zones += z
                        total_presc += p
                        progress.update(
                            dl_task,
                            advance=1,
                            zones=f"{total_zones:,}",
                            presc=f"{total_presc:,}",
                        )

            # --- 2. Merge + Clean + Load ---
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
                transient=True,
            ) as progress:
                task = progress.add_task(f"  Processing {dep}", total=4)

                progress.update(task, description=f"  Merging zones...")
                zones_raw = _merge_local_files(tmp, "zones")
                progress.advance(task)

                progress.update(task, description=f"  Merging prescriptions...")
                presc_raw = _merge_local_files(tmp, "presc")
                progress.advance(task)

                progress.update(task, description=f"  Cleaning...")
                zones = _clean_zones(zones_raw, dep)
                presc = _clean_prescriptions(presc_raw, dep)
                progress.advance(task)

                progress.update(task, description=f"  Loading to PostGIS...")
                _delete_department("zones", dep)
                if not zones.empty:
                    _load_to_postgis(zones, "zones")
                _delete_department("prescriptions", dep)
                if not presc.empty:
                    _load_to_postgis(presc, "prescriptions")
                progress.advance(task)

            if not zones.empty:
                console.print(f"  [green]{len(zones):,} zones loaded[/]")
            else:
                console.print(f"  [yellow]No zones for {dep}[/]")
            if not presc.empty:
                console.print(f"  [green]{len(presc):,} prescriptions loaded[/]")
            else:
                console.print(f"  [yellow]No prescriptions for {dep}[/]")

    console.print(f"\n[bold green]PLU pipeline done[/]")
