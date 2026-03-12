# geo-score-integration

ETL pipelines to load geospatial datasets into the [geo-score-back](../geo-score-back) PostGIS database.

## Setup

```bash
# Install dependencies
uv sync

# Copy and edit config
cp .env.example .env
```

The PostgreSQL/PostGIS database must be running (via geo-score-back's `docker-compose`):

```bash
cd ../geo-score-back/docker && docker compose up -d
```

## Usage

```bash
# Check DB connection
uv run geo-integrate check-db

# Load DVF prices per cadastral section (Paris, 2023)
uv run geo-integrate dvf --year 2023 --dep 75

# Multiple departments
uv run geo-integrate dvf --year 2023 --dep 75 --dep 92 --dep 93 --dep 94

# All departments at once (works for any pipeline)
uv run geo-integrate dvf --year 2023 --all
```

## DVF — Land value prices per cadastral section

Downloads DVF open data + cadastral section geometries from data.gouv.fr, aggregates median price/m² per section, and loads into PostGIS.

**Schema:** `dvf_prices` — one table per year (`y2023`, `y2022`, etc.)

**Columns:** `section_id`, `geom` (native PostGIS with GIST index), `prix_m2_median`, `prix_m2_mean`, `nb_ventes`, `surface_mediane`, `departement`

```bash
uv run geo-integrate dvf --year 2023 --dep 75
uv run geo-integrate dvf --year 2023 --all
uv run geo-integrate dvf --year 2022 --dep 75    # → dvf_prices.y2022
```

## Crime stats — Crime statistics per commune

Downloads commune-level crime statistics from data.gouv.fr + commune geometries from Etalab cadastre, pivots indicators into columns (rate per 1000 + count), and loads into PostGIS.

**Schema:** `crime_stats` — one table per year (`y2024`, `y2023`, etc.)

**Columns:** `code_commune`, `geom` (GIST index), `departement`, `taux_*` (rate per 1000), `nb_*` (count)

```bash
uv run geo-integrate delinquance --year 2024 --dep 75
uv run geo-integrate delinquance --year 2024 --all
```

## OSM Shops — Shops and amenities as points

Queries the Overpass API for `shop=*` and key `amenity=*` tags (restaurants, cafes, pharmacies, banks, etc.) as points.

**Schema:** `osm` — table: `shops`

**Columns:** `osm_id`, `name`, `shop`, `amenity`, `cuisine`, `brand`, `opening_hours`, `addr_*`, `departement`, `geom` (Point, GIST index)

```bash
uv run geo-integrate shops --dep 75
uv run geo-integrate shops --all
```

## OSM Green Spaces — Parks, gardens, playgrounds as polygons

Queries the Overpass API for green/recreational areas: parks, gardens, playgrounds, dog parks, nature reserves, recreation grounds, forests, meadows.

**Schema:** `osm` — table: `green_spaces`

**Columns:** `osm_id`, `osm_type`, `name`, `leisure`, `landuse`, `access`, `surface`, `departement`, `geom` (Polygon/MultiPolygon, GIST index)

```bash
uv run geo-integrate green-spaces --dep 75
uv run geo-integrate green-spaces --all
```

## Pipelines

| Pipeline       | Schema        | Table           | Description                                                           |
|----------------|---------------|-----------------|-----------------------------------------------------------------------|
| `dvf`          | `dvf_prices`  | `y{year}`       | Median price/m² per cadastral section (DVF + Etalab cadastre)         |
| `delinquance`  | `crime_stats` | `y{year}`       | Crime statistics per commune (Ministry of Interior + Etalab cadastre) |
| `shops`        | `osm`         | `shops`         | Shops and amenities as points (Overpass API)                          |
| `green-spaces` | `osm`         | `green_spaces`  | Parks, gardens, playgrounds as polygons (Overpass API)                |

All pipelines support `--dep` (one or more) and `--all` flags. Pipelines are idempotent per department.

## Architecture

```
src/
├── config.py              # Settings from .env (DB credentials)
├── db.py                  # SQLAlchemy engine + session
├── cli.py                 # Typer CLI (entry point)
├── common/
│   ├── download.py        # HTTP file download + gzip decompression
│   ├── loader.py          # PostGIS loading + native geom + GIST index
│   ├── overpass.py        # Overpass API query + retry + department bboxes
│   └── schema.py          # Schema creation + idempotent department upsert
└── pipelines/
    ├── dvf_prices.py      # DVF → cadastral sections ETL
    ├── crime_stats.py     # Crime stats → communes ETL
    ├── osm_shops.py       # OSM shops → points ETL
    └── osm_green_spaces.py # OSM green spaces → polygons ETL
```

## Adding a new pipeline

1. Create `src/pipelines/my_pipeline.py` with a `run()` function
2. Add a command in `src/cli.py`
3. Document the table in this README
