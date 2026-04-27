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

## DVF sections — Cadastral section reference geometries

Downloads all cadastral section geometries from the Etalab cadastre (derived from DGFiP) into a single reference table. Run this once (and refresh yearly); yearly DVF price tables join on `section_id`.

**Schema:** `dvf_prices` — table: `sections`

**Columns:** `section_id` (PK-like), `commune`, `prefixe`, `code`, `departement`, `geom` (Polygon, GIST index)

```bash
uv run geo-integrate dvf-sections --all
uv run geo-integrate dvf-sections --dep 75 --dep 94
```

---

## DVF — Land value prices per cadastral section

Downloads DVF open data from data.gouv.fr, filters ventes / VEFA / adjudications, and aggregates median price/m² per section. Geometry is not stored here — join on `dvf_prices.sections.section_id` to get the polygon.

**Schema:** `dvf_prices` — one table per year (`y2023`, `y2022`, etc.)

**Columns:** `section_id`, `prix_m2_median`, `prix_m2_mean`, `nb_ventes`, `surface_mediane`, `departement`

**Mutation types kept:** `Vente`, `Vente en l'état futur d'achèvement` (VEFA), `Adjudication`.

```bash
# Load sections once first
uv run geo-integrate dvf-sections --all

# Then load prices per year
uv run geo-integrate dvf --year 2023 --dep 75
uv run geo-integrate dvf --year 2023 --all
uv run geo-integrate dvf --year 2021 --year 2022 --year 2023 --all   # multiple years
```

---

## Commune geoms — Shared commune reference layer

Downloads all commune geometries from the Etalab cadastre into a single reference table. Run once (refresh yearly); commune-level pipelines (crime_stats, rents) join on `code_commune`.

**Schema:** `geom_utils` — table: `communes`

**Columns:** `code_commune` (PK-like), `nom_commune`, `departement`, `geom` (Polygon, GIST index)

```bash
uv run geo-integrate commune-geoms --all
uv run geo-integrate commune-geoms --dep 75 --dep 94
```

---

## Crime stats — Crime statistics per commune

Downloads commune-level crime statistics from data.gouv.fr, pivots indicators into columns (rate per 1000 + count). Geometry not stored here — join on `geom_utils.communes.code_commune`.

**Schema:** `crime_stats` — one table per year (`y2024`, `y2023`, etc.)

**Columns:** `code_commune`, `departement`, `taux_*` (rate per 1000), `nb_*` (count)

```bash
# Load communes once first
uv run geo-integrate commune-geoms --all

# Then load crime stats per year
uv run geo-integrate delinquance --year 2024 --dep 75
uv run geo-integrate delinquance --year 2024 --all
```

---

## Rents — Carte des loyers per commune

Downloads ANIL/Ministère de la Transition écologique rent estimates from data.gouv.fr. One row per commune × property type (4 categories).

**Schema:** `rents` — one table per year (`indicators_2025`, etc.)

**Columns:** `code_commune`, `type_local` (`apartment_all` / `apartment_t1_t2` / `apartment_t3_plus` / `house`), `departement`, `loyer_m2_median` (predicted €/m² CC), `loyer_m2_lower`, `loyer_m2_upper` (95% confidence interval bounds), `prediction_type` (`commune` if enough obs, else `maille`), `nb_obs`, `r2_adj`

Reference apartment sizes used by the model: 52 m² (all), 37 m² (T1-T2), 72 m² (T3+), 92 m² (house).

```bash
# Load communes once first
uv run geo-integrate commune-geoms --all

# Then load rents
uv run geo-integrate rents --year 2025 --all
uv run geo-integrate rents --year 2025 --dep 75 --dep 94
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

## BDNB — Base de Données Nationale des Bâtiments

Downloads per-department CSV archives from the BDNB open data (CSTB, millesime 2025-07-a) and loads selected tables into PostGIS. Tables join via `batiment_groupe_id`.

**Schema:** `bati`

| Table | Columns | Description |
|-------|---------|-------------|
| `buildings` | geom, code_iris, code_commune_insee, s_geom_groupe | Central building entity (32M rows nationally) |
| `construction` | annee_construction, mat_mur_txt, mat_toit_txt, nb_niveau, nb_log | Construction characteristics |
| `energy_dpe` | classe_bilan_dpe, classe_emission_ges, conso_5_usages_ep_m2, ... (106 cols) | Representative DPE energy performance |
| `natural_risks` | alea_radon, alea_argile, alea_sismique | Natural risk indicators per building |
| `property_values` | valeur_fonciere, prix_m2_local, prix_m2_terrain, date_mutation | Representative DVF transaction |
| `coproperty` | numero_immat_principal, nb_lot_tot, nb_log, copro_dans_pvd | Copropriete registry (RNC) |
| `social_housing` | classe_ener_principale, nb_log, type_construction | Social housing (RPLS) |
| `addresses` | geom, cle_interop_adr, fiabilite | BAN geocoded addresses |

```bash
uv run geo-integrate bdnb --dep 75
uv run geo-integrate bdnb --dep 75 --dep 92 --dep 93 --dep 94
uv run geo-integrate bdnb --all    # ~35 GB in DB, long download
```

---

## OSM Transport — Train stations, metro, tram, bus stops

Queries the Overpass API for public transport stops and stations as points.

**Schema:** `osm` — table: `transport`

**Columns:** `osm_id`, `name`, `transport_type` (`train_station` / `metro_station` / `tram_stop` / `bus_station` / `bus_stop`), `network`, `operator`, `line`, `departement`, `geom` (Point, GIST index)

```bash
uv run geo-integrate transport --dep 75
uv run geo-integrate transport --all
```

---

## OSM Roads — Road network and parking locations

Queries the Overpass API for roads (motorway, trunk, primary, secondary) and parking facilities.

**Schema:** `osm` — tables: `roads`, `parking`

**Columns (roads):** `osm_id`, `name`, `highway`, `lanes`, `maxspeed`, `surface`, `departement`, `geom` (LineString, GIST index)

**Columns (parking):** `osm_id`, `name`, `parking_type`, `capacity`, `fee`, `access`, `departement`, `geom` (Point/Polygon, GIST index)

```bash
uv run geo-integrate roads --dep 75
uv run geo-integrate roads --all
```

---

## DPE Collectif — Building-level energy performance (ADEME)

Downloads collective building DPE audits from ADEME Open Data API, geocodes via BAN address, and loads into PostGIS.

**Schema:** `energy` — table: `dpe_collectif`

**Columns:** `numero_dpe`, `type_batiment`, `etiquette_dpe`, `etiquette_ges`, `conso_5_usages_par_m2_ep`, `emission_ges_5_usages_par_m2`, `qualite_isolation_*`, `surface_habitable_immeuble`, `nombre_appartement`, `periode_construction`, `cout_total_5_usages`, `date_etablissement_dpe`, `departement`, `geom` (Point, GIST index)

```bash
uv run geo-integrate dpe-collectif --dep 75
uv run geo-integrate dpe-collectif --all
```

---

## OSM Water — Rivers, lakes, canals

Queries the Overpass API for water bodies: rivers, canals, streams, and lakes.

**Schema:** `osm` — table: `water`

**Columns:** `osm_id`, `name`, `water_type` (`river` / `canal` / `stream` / `water`), `departement`, `geom` (LineString/Polygon, GIST index)

```bash
uv run geo-integrate water --dep 75
uv run geo-integrate water --all
```

---

## Climate — Météo-France temperature statistics per station

Downloads Météo-France monthly climate data (MENSQ) from data.gouv.fr, aggregates 10 years (2014–2024) of temperature records per station.

**Schema:** `climate` — table: `stations`

**Columns:** `station_id`, `station_name`, `altitude`, `departement`, `avg_temp_max`, `avg_temp_min`, `max_temp_recorded`, `avg_days_above_25`, `avg_days_above_30`, `avg_days_above_35`, `geom` (Point, GIST index)

```bash
uv run geo-integrate climate --dep 75
uv run geo-integrate climate --all
```

---

## Coastal Erosion — Shoreline evolution indicators (Cerema)

Downloads national coastal erosion shapefile from Cerema/Géolittoral, tracking 50+ years of shoreline evolution. Reprojects from Lambert 93 to WGS84.

**Schema:** `coastal` — table: `erosion`

```bash
uv run geo-integrate coastal-erosion
```

---

## ICU — Urban Heat Island indicators (CSTB)

Downloads satellite-derived urban heat island (ICU) indicators from CSTB Sat4BDNB dataset.

**Schema:** `climate` — table: `icu`

```bash
uv run geo-integrate icu
```

---

## Air Quality — ATMO air quality index per commune

Fetches daily ATMO air quality index from Atmo France API with pollutant-specific indices.

**Schema:** `climate` — table: `air_quality`

**Columns:** `commune_code`, `commune_name`, `quality_index`, `quality_label`, `no2_index`, `o3_index`, `pm10_index`, `pm25_index`, `so2_index`, `source`, `date`, `geom` (Point, GIST index)

```bash
uv run geo-integrate air-quality
```

---

## Pollens — Pollen index per commune

Fetches daily pollen index from Atmo France API with species-specific levels.

**Schema:** `climate` — table: `pollens`

**Columns:** `commune_code`, `commune_name`, `quality_index`, `quality_label`, `alert`, `responsible_pollen`, `birch_index`, `grass_index`, `olive_index`, `ragweed_index`, `mugwort_index`, `alder_index`, `date`, `source`, `geom` (Point, GIST index)

```bash
uv run geo-integrate pollens
```

---

## PLU — Plan Local d'Urbanisme (zoning + prescriptions)

Fetches PLU zoning and prescriptions from the Géoportail de l'Urbanisme (GPU) WFS service. Pre-checks each commune's document status (PLU/PLUi/CC vs RNU) via `wfs_du:document` and only downloads data for covered communes. Downloads run in parallel (5 workers).

**Schema:** `plu` — tables: `zones`, `prescriptions`

**Columns (zones):** `typezone`, `zone_category` (`urban` / `to_urbanize` / `to_urbanize_strict` / `agricultural` / `natural` / `other`), `libelle`, `libelong`, `destdomi`, `idurba`, `datappro`, `urlfic`, `departement`, `geom` (Polygon, GIST index)

**Columns (prescriptions):** `typepsc`, `stypepsc`, `libelle`, `txt`, `departement`, `geom` (Polygon, GIST index)

**Zone categories:**
- `urban` — Urbaine / constructible (typezone U)
- `to_urbanize` — À Urbaniser / court terme (typezone AU, AUc)
- `to_urbanize_strict` — À Urbaniser / strict, requires modification (typezone AUs)
- `agricultural` — Agricole / non constructible sauf dérogation (typezone A)
- `natural` — Naturelle / non constructible (typezone N)
- `other` — unrecognized typezone

**Document status:** communes are pre-checked against the GPU document registry. Only communes with a PLU, PLUi, or CC are downloaded. Communes under RNU (no local planning document) are skipped.

```bash
uv run geo-integrate plu --dep 75
uv run geo-integrate plu --all
```

---

## Wiki POIs — Points of interest enriched with Wikidata

Queries Overpass API for OSM nodes with a `wikidata` tag (tourism, historic, amenity, leisure categories), then enriches each POI with Wikidata descriptions and image URLs.

**Schema:** `osm` — table: `poi_wiki`

**Columns:** `osm_id`, `wikidata_id`, `name`, `category`, `description`, `image_url`, `departement`, `geom` (Point, GIST index)

**Categories queried:**
- `tourism` — museum, gallery, hotel, attraction, viewpoint
- `historic` — monument, castle, memorial, ruins, archaeological site, church
- `amenity` — restaurant, cafe, bar, theatre, cinema, museum, place of worship
- `leisure` — park, garden, sports centre, stadium

```bash
uv run geo-integrate wiki-pois --dep 75
uv run geo-integrate wiki-pois --all
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
| `bdnb`         | `bati`        | 8 tables                      | BDNB (CSTB)       | Building data: energy, risks, DVF, copro |
| `transport`    | `osm`         | `transport`   | Overpass API             | Train stations, metro, tram, bus stops             |
| `roads`        | `osm`         | `roads` / `parking` | Overpass API       | Road network and parking locations                 |
| `dpe-collectif`| `energy`      | `dpe_collectif` | ADEME Open Data        | Building-level energy performance (DPE)            |
| `water`        | `osm`         | `water`       | Overpass API             | Rivers, lakes, canals                              |
| `climate`      | `climate`     | `stations`    | Météo-France             | Temperature statistics per station (10y avg)        |
| `coastal-erosion` | `coastal`  | `erosion`     | Cerema/Géolittoral       | Shoreline evolution indicators                     |
| `icu`          | `climate`     | `icu`         | CSTB Sat4BDNB            | Urban heat island indicators                       |
| `air-quality`  | `climate`     | `air_quality` | Atmo France API          | ATMO air quality index per commune                 |
| `pollens`      | `climate`     | `pollens`     | Atmo France API          | Pollen index per commune                           |
| `plu`          | `plu`         | `zones` / `prescriptions` | GPU WFS      | PLU zoning and prescriptions                       |
| `wiki-pois`    | `osm`         | `poi_wiki`    | Overpass + Wikidata      | POIs enriched with descriptions and images         |

All pipelines support `--dep` (one or more) and `--all` flags unless noted otherwise. Pipelines are idempotent per department.

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
    ├── osm_transport.py       # OSM transport stops → points ETL
    ├── osm_roads.py           # OSM roads + parking ETL
    ├── osm_water.py           # OSM water bodies ETL
    ├── mnt_exposure.py        # Copernicus DEM → sun exposure polygons ETL
    ├── flood_tri.py           # Georisques TRI → flood zone polygons ETL
    ├── clay_risk.py           # Georisques RGA → clay shrink-swell zones ETL
    ├── storm_risk.py          # Eurocode wind zones + GASPAR CatNat storms ETL
    ├── bdnb.py                # BDNB building data (energy, risks, DVF, copro) ETL
    ├── dpe_collectif.py       # ADEME DPE collectif ETL
    ├── climate.py             # Météo-France temperature stats ETL
    ├── coastal_erosion.py     # Cerema coastal erosion ETL
    ├── icu.py                 # CSTB urban heat island ETL
    ├── air_quality.py         # Atmo air quality index ETL
    ├── pollens.py             # Atmo pollen index ETL
    ├── plu.py                 # GPU PLU zoning + prescriptions ETL
    └── wiki_pois.py           # Wikidata-enriched POIs ETL
docker/
├── docker-compose.yml         # PostGIS database (shared with geo-score-back)
└── init-db/01-init.sql        # PostGIS extensions init
```

## Adding a new pipeline

1. Create `src/pipelines/my_pipeline.py` with a `run(departements: list[str])` function
2. Add a command in `src/settings/cli.py`
3. Document the pipeline in this README with: description, schema/table, columns, and example commands
