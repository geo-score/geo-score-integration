"""PostGIS loading with native geometry conversion and spatial index."""

import geopandas as gpd
from sqlalchemy import text

from settings.db import engine

 
def load_geodataframe(
        gdf: gpd.GeoDataFrame,
        table: str,
        schema: str,
        *,
        geom_type: str = "Geometry",
):
    """Load a GeoDataFrame into PostGIS with native geom column and GIST index."""
    qualified = f"{schema}.{table}"

    gdf.to_postgis(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        dtype={"geometry": "Geometry"},
    )

    with engine.connect() as conn:
        conn.execute(text(
            f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS geom geometry({geom_type}, 4326)"
        ))
        conn.execute(text(
            f"UPDATE {qualified} SET geom = geometry::geometry WHERE geom IS NULL"
        ))
        conn.execute(text(
            f"ALTER TABLE {qualified} DROP COLUMN IF EXISTS geometry"
        ))
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_geom ON {qualified} USING GIST (geom)"
        ))
        conn.commit()
