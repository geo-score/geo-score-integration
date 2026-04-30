# geo-score-integration

ETL pipelines to load geospatial datasets into the **geo_score** PostGIS database.
Shares the same database as **geo-score-back** (the API backend, sibling project).

## Quick start

```bash
# 1. Start the shared PostGIS database
cd docker && docker compose up -d && cd ..

# 2. Install dependencies
uv sync

# 3. Copy env (defaults match docker-compose)
cp .env.example .env

# 4. Check connection
uv run geo-integrate check-db

# 5. Load data (example: DVF prices for Paris)
uv run geo-integrate dvf --dep 75
```

## Project structure

```
src/
  settings/       # Config (Pydantic), DB engine (SQLAlchemy), CLI (Typer)
  common/         # Shared: download, PostGIS loader, schema management, Overpass API
  pipelines/      # One file per data source (ETL pipeline)
docker/           # docker-compose + PostGIS init scripts (shared with geo-score-back)
```

## Pipelines

| CLI command      | Pipeline file        | Schema        | Table          | Geometry   |
|------------------|----------------------|---------------|----------------|------------|
| `commune-geoms`  | `commune_geoms.py`   | `geom_utils`  | `communes`     | Polygon    |
| `dvf-sections`   | `dvf_sections.py`    | `dvf_prices`  | `sections`     | Polygon    |
| `dvf`            | `dvf_prices.py`      | `dvf_prices`  | `y{year}` + `communes_y{year}` | (no geom, FK `section_id` / `code_commune`) |
| `dvf-communes`   | `dvf_communes.py`    | `dvf_prices`  | `communes_y{year}` (only)      | (no geom, FK `code_commune`) |
| `delinquance`    | `crime_stats.py`     | `crime_stats` | `y{year}`      | (no geom, FK `code_commune`) |
| `rents`          | `rents.py`           | `rents`       | `indicators_{year}` | (no geom, FK `code_commune`) |
| `shops`          | `osm_shops.py`       | `osm`         | `shops`        | Point      |
| `nightclubs`     | `osm_nightclubs.py`  | `osm`         | `nightclubs`   | Point      |
| `railways`       | `osm_railways.py`    | `osm`         | `railways` / `railway_stations` | Line / Point |
| `airports`       | `osm_airports.py`    | `osm`         | `airports`     | Polygon / Line / Point |
| `industry`       | `osm_industry.py`    | `osm`         | `industry`     | Polygon    |
| `green-spaces`   | `osm_green_spaces.py`| `osm`         | `green_spaces` | Polygon    |
| `exposition`     | `mnt_exposure.py`    | `mnt`         | `exposure`     | Polygon    |
| `flood-tri`      | `flood_tri.py`       | `flood_risk`  | `tri_zones`    | Polygon    |
| `clay-risk`      | `clay_risk.py`       | `clay_risk`   | `rga_zones`    | Polygon    |
| `storm-risk`     | `storm_risk.py`      | `storm_risk`  | `wind_zones` / `catnat_storm` | Polygon |
| `bdnb`           | `bdnb.py`            | `bati`        | 8 tables                      | Polygon + flat |
| `transport`      | `osm_transport.py`   | `osm`         | `transport`                   | Point      |
| `roads`          | `osm_roads.py`       | `osm`         | `roads` / `parking`           | Line / Polygon |
| `dpe-collectif`  | `dpe_collectif.py`   | `energy`      | `dpe_collectif`               | Point      |
| `water`          | `osm_water.py`       | `osm`         | `water`                       | Line / Polygon |
| `climate`        | `climate.py`         | `climate`     | `stations`                    | Point      |
| `coastal-erosion`| `coastal_erosion.py` | `coastal`     | `erosion`                     | Polygon    |
| `icu`            | `icu.py`             | `climate`     | `icu`                         | Polygon    |
| `air-quality`    | `air_quality.py`     | `climate`     | `air_quality`                 | Point      |
| `pollens`        | `pollens.py`         | `climate`     | `pollens`                     | Point      |
| `plu`            | `plu.py`             | `plu`         | `zones` / `prescriptions`     | Polygon    |
| `wiki-pois`      | `wiki_pois.py`       | `osm`         | `poi_wiki`                    | Point      |

All commands support `--dep 75 --dep 92` or `--all` for department selection.

## Database

- PostgreSQL + PostGIS (docker-compose in `docker/`)
- Geometry column: `geom`, SRID 4326, GIST indexed
- Idempotent loading: delete-by-department + insert

## Conventions

- **All data-facing names in English** (column names, enum values, table names)
- **Every pipeline must be documented in README.md** with: description, schema/table, columns, classification details if any, and example CLI commands
- Pipeline pattern: download → parse/compute → GeoDataFrame → PostGIS
- Dependencies managed with `uv`
- Entry point: `geo-integrate = "settings.cli:app"`
