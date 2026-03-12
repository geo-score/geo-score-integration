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
```

## DVF — Land value prices per cadastral section

Downloads DVF open data + cadastral section geometries from data.gouv.fr, aggregates median price/m² per section, and loads into PostGIS.

**Schema:** `dvf_prices` — one table per year (`y2023`, `y2022`, etc.)

**Columns:** `section_id`, `geom` (native PostGIS with GIST index), `prix_m2_median`, `prix_m2_mean`, `nb_ventes`, `surface_mediane`, `departement`

```bash
# Single department (Paris, 2023)
uv run geo-integrate dvf --year 2023 --dep 75

# Île-de-France
uv run geo-integrate dvf --year 2023 --dep 75 --dep 92 --dep 93 --dep 94 --dep 77 --dep 78 --dep 91 --dep 95

# Different year → separate table (dvf_prices.y2022)
uv run geo-integrate dvf --year 2022 --dep 75

# Add departments to an existing year (idempotent — re-running same dep replaces its data)
uv run geo-integrate dvf --year 2023 --dep 13
uv run geo-integrate dvf --year 2023 --dep 69
```

## Crime stats — Crime statistics per commune

Downloads commune-level crime statistics from data.gouv.fr + commune geometries from Etalab cadastre, pivots indicators into columns (rate per 1000 + count), and loads into PostGIS.

**Schema:** `crime_stats` — one table per year (`y2024`, `y2023`, etc.)

**Columns:** `code_commune`, `geom` (native PostGIS with GIST index), `departement`, plus per-indicator columns:
- `taux_*` — rate per 1000 inhabitants
- `nb_*` — absolute count

Indicators include: burglaries, voluntary damage, domestic violence, non-domestic violence, sexual violence, theft (various types), drug trafficking, drug use, fraud.

```bash
# Single department (Paris, 2024)
uv run geo-integrate delinquance --year 2024 --dep 75

# Île-de-France
uv run geo-integrate delinquance --year 2024 --dep 75 --dep 92 --dep 93 --dep 94 --dep 77 --dep 78 --dep 91 --dep 95

# Different year → separate table (crime_stats.y2023)
uv run geo-integrate delinquance --year 2023 --dep 75

# Idempotent — re-running same dep replaces its data
uv run geo-integrate delinquance --year 2024 --dep 13
```

## OSM Shops — Shops and amenities from OpenStreetMap

Queries the Overpass API for `shop=*` and key `amenity=*` tags (restaurants, cafes, pharmacies, banks, etc.) as points, and loads into PostGIS. Tables are snapshots by date.

**Schema:** `osm_shops` — one table per snapshot date (`d2026_03_12`, etc.)

**Columns:** `osm_id`, `osm_type`, `name`, `shop`, `amenity`, `cuisine`, `brand`, `opening_hours`, `addr_street`, `addr_housenumber`, `addr_postcode`, `addr_city`, `departement`, `geom` (Point, GIST index)

```bash
# Single department (Paris, today's snapshot)
uv run geo-integrate shops --dep 75

# Île-de-France
uv run geo-integrate shops --dep 75 --dep 92 --dep 93 --dep 94 --dep 77 --dep 78 --dep 91 --dep 95

# Specific snapshot date
uv run geo-integrate shops --dep 75 --snapshot 2026-03-01

# Idempotent — re-running same dep + date replaces its data
uv run geo-integrate shops --dep 13
```

## Pipelines

| Pipeline | Schema | Description |
|----------|--------|-------------|
| `dvf` | `dvf_prices` | Median price/m² per cadastral section (DVF + Etalab cadastre) |
| `delinquance` | `crime_stats` | Crime statistics per commune (Ministry of Interior + Etalab cadastre) |
| `shops` | `osm_shops` | Shops and amenities as points (OpenStreetMap / Overpass API) |

## Architecture

```
src/integration/
├── config.py              # Configuration (DATABASE_URL via .env)
├── db.py                  # SQLAlchemy engine + session
├── cli.py                 # Typer CLI (entry point)
└── pipelines/
    ├── dvf_prices.py      # DVF → cadastral sections ETL
    ├── crime_stats.py     # Crime stats → communes ETL
    └── osm_shops.py       # OSM shops → points ETL
```

## Adding a new pipeline

1. Create `src/integration/pipelines/my_pipeline.py` with a `run()` function
2. Add a command in `cli.py`
3. Document the created table in this README
