"""PostGIS loading with native geometry conversion and spatial index."""

import geopandas as gpd
from rich.console import Console
from sqlalchemy import text

from settings.db import engine

console = Console()


def load_geodataframe(
        gdf: gpd.GeoDataFrame,
        table: str,
        schema: str,
        *,
        geom_type: str = "Geometry",
        chunksize: int = 1000,
):
    """Load a GeoDataFrame into PostGIS with native geom column and GIST index."""
    qualified = f"{schema}.{table}"

    if gdf.geometry.name != "geom":
        gdf = gdf.rename_geometry("geom")

    total = len(gdf)
    for start in range(0, total, chunksize):
        chunk = gdf.iloc[start:start + chunksize]
        chunk.to_postgis(
            table,
            engine,
            schema=schema,
            if_exists="append",
            index=False,
        )
        console.print(f"  ... {min(start + chunksize, total)}/{total} rows")

    with engine.connect() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {qualified} USING GIST (geom)"
        ))
        conn.commit()
