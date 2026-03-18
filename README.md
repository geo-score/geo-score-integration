# geo-score-integration

ETL pipelines to load geospatial datasets into the [geo-score-back](../geo-score-back) PostGIS database.

## Setup

```bash
# 1. Start the shared PostGIS database
cd docker && docker compose up -d && cd ..

# 2. Install dependencies
uv sync

# 3. Copy and edit config (defaults match docker-compose)
cp .env.example .env

# 4. Check connection
uv run geo-integrate check-db
```

## Usage

```bash
# Single department
uv run geo-integrate dvf --year 2023 --dep 75

# Multiple departments
uv run geo-integrate dvf --year 2023 --dep 75 --dep 92 --dep 93 --dep 94

# All departments at once (works for any pipeline)
uv run geo-integrate dvf --year 2023 --all
```

---

## DVF — Land value prices per cadastral section

Downloads DVF open data + cadastral section geometries from data.gouv.fr, aggregates median price/m² per section, and loads into PostGIS.

**Schema:** `dvf_prices` — one table per year (`y2023`, `y2022`, etc.)

**Columns:** `section_id`, `geom` (Polygon, GIST index), `prix_m2_median`, `prix_m2_mean`, `nb_ventes`, `surface_mediane`, `departement`

```bash
uv run geo-integrate dvf --year 2023 --dep 75
uv run geo-integrate dvf --year 2023 --all
uv run geo-integrate dvf --year 2022 --dep 75    # → dvf_prices.y2022
```

---

## Crime stats — Crime statistics per commune

Downloads commune-level crime statistics from data.gouv.fr + commune geometries from Etalab cadastre, pivots indicators into columns (rate per 1000 + count), and loads into PostGIS.

**Schema:** `crime_stats` — one table per year (`y2024`, `y2023`, etc.)

**Columns:** `code_commune`, `geom` (Polygon, GIST index), `departement`, `taux_*` (rate per 1000), `nb_*` (count)

```bash
uv run geo-integrate delinquance --year 2024 --dep 75
uv run geo-integrate delinquance --year 2024 --all
```

---

## OSM Shops — Shops and amenities as points

Queries the Overpass API for `shop=*` and key `amenity=*` tags (restaurants, cafes, pharmacies, banks, etc.) as points.

**Schema:** `osm` — table: `shops`

**Columns:** `osm_id`, `name`, `shop`, `amenity`, `cuisine`, `brand`, `opening_hours`, `addr_*`, `departement`, `geom` (Point, GIST index)

```bash
uv run geo-integrate shops --dep 75
uv run geo-integrate shops --all
```

---

## OSM Green Spaces — Parks, gardens, playgrounds as polygons

Queries the Overpass API for green/recreational areas: parks, gardens, playgrounds, dog parks, nature reserves, recreation grounds, forests, meadows.

**Schema:** `osm` — table: `green_spaces`

**Columns:** `osm_id`, `osm_type`, `name`, `leisure`, `landuse`, `access`, `surface`, `departement`, `geom` (Polygon/MultiPolygon, GIST index)

```bash
uv run geo-integrate green-spaces --dep 75
uv run geo-integrate green-spaces --all
```

---

## MNT Exposure — Sun exposure classification from DEM

Downloads Copernicus GLO-90 DEM tiles (90m resolution, AWS Open Data, no auth), computes slope aspect, and classifies sun exposure into vectorised polygons (adjacent cells of same class are merged).

**Schema:** `mnt` — table: `exposure`

**Columns:** `exposition` (`high_exposure` / `low_exposure` / `moderate` / `flat`), `departement`, `geom` (Polygon, GIST index)

**Classification:**
- `high_exposure` — south-facing slope (135°–225°), slope ≥ 2°
- `low_exposure` — north-facing slope (315°–360° / 0°–45°), slope ≥ 2°
- `moderate` — east/west-facing slope, slope ≥ 2°
- `flat` — terrain with slope < 2°

```bash
uv run geo-integrate exposition --dep 75
uv run geo-integrate exposition --all
```

---

## TRI Flood Zones — Flood risk areas (Directive Inondation 2020)

Downloads TRI (Territoires à Risques Importants d'inondation) shapefiles from Georisques, extracts flood zone polygons for three probability scenarios, reprojects from Lambert 93 to WGS84, and loads into PostGIS. Not all departments have TRI data — only those with designated flood risk territories.

**Schema:** `flood_risk` — table: `tri_zones`

**Columns:** `flood_type`, `scenario`, `watercourse`, `tri_id`, `departement`, `geom` (Polygon, GIST index)

**Classification:**
- `flood_type`: `river_overflow` / `runoff` / `marine_submersion`
- `scenario`: `high_probability` (frequent/decadal) / `medium_probability` (centennial) / `medium_probability_climate_change` / `low_probability` (rare/millennial)

```bash
uv run geo-integrate flood-tri --dep 75
uv run geo-integrate flood-tri --dep 13    # multiple TRI zones (Avignon, Marseille, etc.)
uv run geo-integrate flood-tri --all
```

---

## RGA Clay Risk — Clay shrink-swell exposure zones

Downloads the national RGA (retrait-gonflement des argiles) shapefile from Georisques/BRGM (~594 MB, 122K polygons), filters by department, reprojects from Lambert 93 to WGS84, and loads into PostGIS. Areas not covered by any polygon have residual (negligible) exposure.

**Schema:** `clay_risk` — table: `rga_zones`

**Columns:** `exposure_level`, `departement`, `geom` (Polygon, GIST index)

**Classification:**
- `high` — strong exposure to clay shrink-swell
- `medium` — medium exposure
- `low` — low exposure

```bash
uv run geo-integrate clay-risk --dep 92 --dep 93 --dep 94
uv run geo-integrate clay-risk --all
```

---

## Storm Risk — Eurocode wind zones + CatNat storm history

Loads two complementary datasets into the `storm_risk` schema:

### Wind zones (`storm_risk.wind_zones`)

Eurocode EN 1991-1-4 wind zones dividing France into 4 levels based on reference wind speed. 19 polygons covering metropolitan France and overseas territories.

**Columns:** `wind_zone` (1–4), `wind_speed_ms` (22/24/26/28 m/s), `geom` (MultiPolygon, GIST index)

### CatNat storm history (`storm_risk.catnat_storm`)

Historical CatNat (catastrophe naturelle) declarations for storms per commune, aggregated from the GASPAR database (~16K declarations since 1982). Joined to commune geometries from Etalab cadastre.

**Columns:** `code_commune`, `storm_count`, `first_event`, `last_event`, `departement`, `geom` (Polygon, GIST index)

```bash
uv run geo-integrate storm-risk --dep 29 --dep 33
uv run geo-integrate storm-risk --all
```

---

## Pipelines summary

| Pipeline       | Schema        | Table           | Source                   | Description                                        |
|----------------|---------------|-----------------|--------------------------|----------------------------------------------------|
| `dvf`          | `dvf_prices`  | `y{year}`       | data.gouv.fr             | Median price/m² per cadastral section              |
| `delinquance`  | `crime_stats` | `y{year}`       | data.gouv.fr             | Crime statistics per commune                       |
| `shops`        | `osm`         | `shops`         | Overpass API             | Shops and amenities as points                      |
| `green-spaces` | `osm`         | `green_spaces`  | Overpass API             | Parks, gardens, playgrounds as polygons            |
| `exposition`   | `mnt`         | `exposure`      | Copernicus DEM 90m (AWS) | Sun exposure classification from slope aspect      |
| `flood-tri`    | `flood_risk`  | `tri_zones`     | Georisques               | TRI flood zones with probability scenarios         |
| `clay-risk`    | `clay_risk`   | `rga_zones`     | Georisques / BRGM        | Clay shrink-swell exposure zones                   |
| `storm-risk`   | `storm_risk`  | `wind_zones` / `catnat_storm` | Eurocode + GASPAR | Wind zones + storm CatNat history     |

All pipelines support `--dep` (one or more) and `--all` flags. Pipelines are idempotent per department.

## Architecture

```
src/
├── settings/
│   ├── config.py              # Settings from .env (DB credentials)
│   ├── db.py                  # SQLAlchemy engine + session
│   └── cli.py                 # Typer CLI (entry point)
├── common/
│   ├── download.py            # HTTP file download + gzip decompression
│   ├── loader.py              # PostGIS loading + native geom + GIST index
│   ├── overpass.py            # Overpass API query + retry + department bboxes
│   └── schema.py              # Schema creation + idempotent department upsert
└── pipelines/
    ├── dvf_prices.py          # DVF → cadastral sections ETL
    ├── crime_stats.py         # Crime stats → communes ETL
    ├── osm_shops.py           # OSM shops → points ETL
    ├── osm_green_spaces.py    # OSM green spaces → polygons ETL
    ├── mnt_exposure.py        # Copernicus DEM → sun exposure polygons ETL
    ├── flood_tri.py           # Georisques TRI → flood zone polygons ETL
    ├── clay_risk.py           # Georisques RGA → clay shrink-swell zones ETL
    └── storm_risk.py          # Eurocode wind zones + GASPAR CatNat storms ETL
docker/
├── docker-compose.yml         # PostGIS database (shared with geo-score-back)
└── init-db/01-init.sql        # PostGIS extensions init
```

## Adding a new pipeline

1. Create `src/pipelines/my_pipeline.py` with a `run(departements: list[str])` function
2. Add a command in `src/settings/cli.py`
3. Document the pipeline in this README with: description, schema/table, columns, and example commands
