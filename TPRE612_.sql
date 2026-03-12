-- ============================================================
-- ETL: TPRE612_DATASET_CLEAN → TPRE612_DATA_WAREHOUSE
-- Run order matters (FK dependencies respected)
-- ============================================================

-- -------------------------------------------------------
-- 0. Create schema + tables (idempotent)
-- -------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS "tpre612_data_warehouse";

CREATE TABLE IF NOT EXISTS "tpre612_data_warehouse"."dim_operateur" (
    agency_id       TEXT PRIMARY KEY,
    agency_name     TEXT,
    agency_url      TEXT,
    agency_timezone TEXT,
    agency_lang     TEXT
);

CREATE TABLE IF NOT EXISTS "tpre612_data_warehouse"."dim_route" (
    route_id        INT PRIMARY KEY,
    agency_id       TEXT REFERENCES "tpre612_data_warehouse"."dim_operateur"(agency_id),
    route_long_name TEXT,
    origin          TEXT,
    destination     TEXT,
    countries       TEXT
);

CREATE TABLE IF NOT EXISTS "tpre612_data_warehouse"."dim_train" (
    trip_id                  TEXT PRIMARY KEY,
    route_id                 INT REFERENCES "tpre612_data_warehouse"."dim_route"(route_id),
    trip_headsign            TEXT,
    trip_origin              TEXT,
    destination_arrival_time TIME,
    duration                 INTERVAL,
    distance                 FLOAT
);

CREATE TABLE IF NOT EXISTS "tpre612_data_warehouse"."dim_gare" (
    gare_id          SERIAL PRIMARY KEY,
    name             TEXT,
    city             TEXT,
    country          TEXT,
    latitude         FLOAT,
    longitude        FLOAT,
    is_main_station  BOOLEAN
);

CREATE TABLE IF NOT EXISTS "tpre612_data_warehouse"."dim_date" (
    date_id    SERIAL PRIMARY KEY,
    start_date DATE,
    end_date   DATE,
    monday     BOOLEAN,
    tuesday    BOOLEAN,
    wednesday  BOOLEAN,
    thursday   BOOLEAN,
    friday     BOOLEAN,
    saturday   BOOLEAN,
    sunday     BOOLEAN
);

CREATE TABLE IF NOT EXISTS "tpre612_data_warehouse"."dim_energie" (
    energy_id    SERIAL PRIMARY KEY,
    geo          TEXT,
    vehicle      TEXT,
    energy_type  TEXT,
    year         INT,
    energy_value FLOAT
);

CREATE TABLE IF NOT EXISTS "tpre612_data_warehouse"."fact_trajet_train" (
    fact_id       SERIAL PRIMARY KEY,
    train_id      TEXT    REFERENCES "tpre612_data_warehouse"."dim_train"(trip_id),
    route_id      INT     REFERENCES "tpre612_data_warehouse"."dim_route"(route_id),
    operator_id   TEXT    REFERENCES "tpre612_data_warehouse"."dim_operateur"(agency_id),
    gare_depart_id  INT   REFERENCES "tpre612_data_warehouse"."dim_gare"(gare_id),
    gare_arrivee_id INT   REFERENCES "tpre612_data_warehouse"."dim_gare"(gare_id),
    date_id         INT   REFERENCES "tpre612_data_warehouse"."dim_date"(date_id),
    distance_km     FLOAT,
    duree_minutes   FLOAT,
    emissions_co2   FLOAT,
    passengers      FLOAT,
    average_speed   FLOAT
);

-- -------------------------------------------------------
-- 1. dim_operateur  (source: agencies)
-- -------------------------------------------------------
INSERT INTO "tpre612_data_warehouse"."dim_operateur"
    (agency_id, agency_name, agency_url, agency_timezone, agency_lang)
SELECT DISTINCT ON (agency_id)
    agency_id,
    agency_name,
    agency_url,
    agency_timezone,
    agency_lang
FROM "tpre612_dataset_clean"."agencies"
WHERE agency_id IS NOT NULL
ON CONFLICT (agency_id) DO NOTHING;


-- -------------------------------------------------------
-- 2. dim_route  (source: routes  JOIN  agencies)
-- -------------------------------------------------------
INSERT INTO "tpre612_data_warehouse"."dim_route"
    (route_id, agency_id, route_long_name, origin, destination, countries)
SELECT DISTINCT ON (r.route_id)
    NULLIF(TRIM(r.route_id::TEXT),'')::FLOAT::INT,
    r.agency_id,
    r.route_long_name,
    r.origin_trip_0   AS origin,
    r.destination_trip_0 AS destination,
    r.countries
FROM "tpre612_dataset_clean"."routes" r
WHERE r.route_id IS NOT NULL
  AND r.agency_id IN (
      SELECT agency_id FROM "tpre612_data_warehouse"."dim_operateur"
  )
ON CONFLICT (route_id) DO NOTHING;


-- -------------------------------------------------------
-- 3. dim_train  (source: trips)
-- -------------------------------------------------------
INSERT INTO "tpre612_data_warehouse"."dim_train"
    (trip_id, route_id, trip_headsign, trip_origin,
     destination_arrival_time, duration, distance)
SELECT DISTINCT ON (t.trip_id)
    t.trip_id,
    NULLIF(TRIM(t.route_id::TEXT),'')::FLOAT::INT,
    t.trip_headsign,
    t.trip_origin,
    -- safe text → TIME / INTERVAL cast (nullify unparseable values)
    CASE WHEN t.destination_arrival_time ~ '^\d{2}:\d{2}(:\d{2})?$'
         THEN NULLIF(TRIM(t.destination_arrival_time),'')::TIME
         ELSE NULL END AS destination_arrival_time,
    NULLIF(TRIM(t.duration::TEXT),'')::INTERVAL AS duration,
    NULLIF(TRIM(t.distance::TEXT),'')::FLOAT
FROM "tpre612_dataset_clean"."trips" t
WHERE t.trip_id IS NOT NULL
  AND NULLIF(TRIM(t.route_id::TEXT),'')::FLOAT::INT IN (
      SELECT route_id FROM "tpre612_data_warehouse"."dim_route"
  )
ON CONFLICT (trip_id) DO NOTHING;


-- -------------------------------------------------------
-- 4. dim_gare  (source: gares_europeennes + stops)
--    Merge both sources; use COALESCE for missing fields.
-- -------------------------------------------------------
INSERT INTO "tpre612_data_warehouse"."dim_gare"
    (name, city, country, latitude, longitude, is_main_station)

-- 4a. From European stations master (data.gouv.fr)
SELECT DISTINCT ON (name, country)
    g.name,
    g.name                  AS city,   -- no separate city field; name is station name
    g.country,
    NULLIF(TRIM(g.latitude::TEXT),'')::FLOAT,
    NULLIF(TRIM(g.longitude::TEXT),'')::FLOAT,
    (g.is_main_station = 't') AS is_main_station
FROM "tpre612_dataset_clean"."gares_europeennes" g
WHERE g.latitude  IS NOT NULL
  AND g.longitude IS NOT NULL
  AND g.name      IS NOT NULL

UNION ALL

-- 4b. From night-train stops (back-on-track)
SELECT DISTINCT ON (s.stop_name, s.stop_country)
    s.stop_name             AS name,
    COALESCE(s.stop_cityname, s.stop_name) AS city,
    s.stop_country          AS country,
    NULLIF(TRIM(s.stop_lat::TEXT),'')::FLOAT,
    NULLIF(TRIM(s.stop_lon::TEXT),'')::FLOAT,
    FALSE                   AS is_main_station
FROM "tpre612_dataset_clean"."stops" s
WHERE s.stop_lat  IS NOT NULL
  AND s.stop_lon  IS NOT NULL
  AND s.stop_name IS NOT NULL;
-- Note: dim_gare uses SERIAL PK — no ON CONFLICT needed.


-- -------------------------------------------------------
-- 5. dim_date  (source: view_ontd_details)
--    Two service periods per route (trip_0 / trip_1).
-- -------------------------------------------------------
INSERT INTO "tpre612_data_warehouse"."dim_date"
    (start_date, end_date,
     monday, tuesday, wednesday, thursday, friday, saturday, sunday)

SELECT DISTINCT
    NULLIF(TRIM(start_date_0::TEXT),'')::DATE      AS start_date,
    NULLIF(TRIM(end_date_0::TEXT),'')::DATE        AS end_date,
    (NULLIF(TRIM(monday_0::TEXT),'')::FLOAT::INT = 1)       AS monday,
    (NULLIF(TRIM(tuesday_0::TEXT),'')::FLOAT::INT = 1)       AS tuesday,
    (NULLIF(TRIM(wednesday_0::TEXT),'')::FLOAT::INT = 1)       AS wednesday,
    (NULLIF(TRIM(thursday_0::TEXT),'')::FLOAT::INT = 1)       AS thursday,
    (NULLIF(TRIM(friday_0::TEXT),'')::FLOAT::INT = 1)       AS friday,
    (NULLIF(TRIM(saturday_0::TEXT),'')::FLOAT::INT = 1)       AS saturday,
    (NULLIF(TRIM(sunday_0::TEXT),'')::FLOAT::INT = 1)       AS sunday
FROM "tpre612_dataset_clean"."view_ontd_details"
WHERE start_date_0 IS NOT NULL
  AND end_date_0   IS NOT NULL

UNION

SELECT DISTINCT
    NULLIF(TRIM(start_date_1::TEXT),'')::DATE,
    NULLIF(TRIM(end_date_1::TEXT),'')::DATE,
    (NULLIF(TRIM(monday_1::TEXT),'')::FLOAT::INT = 1),
    (NULLIF(TRIM(tuesday_1::TEXT),'')::FLOAT::INT = 1),
    (NULLIF(TRIM(wednesday_1::TEXT),'')::FLOAT::INT = 1),
    (NULLIF(TRIM(thursday_1::TEXT),'')::FLOAT::INT = 1),
    (NULLIF(TRIM(friday_1::TEXT),'')::FLOAT::INT = 1),
    (NULLIF(TRIM(saturday_1::TEXT),'')::FLOAT::INT = 1),
    (NULLIF(TRIM(sunday_1::TEXT),'')::FLOAT::INT = 1)
FROM "tpre612_dataset_clean"."view_ontd_details"
WHERE start_date_1 IS NOT NULL
  AND end_date_1   IS NOT NULL;


-- -------------------------------------------------------
-- 6. dim_energie  (source: train_traffic_source_energy)
-- -------------------------------------------------------
INSERT INTO "tpre612_data_warehouse"."dim_energie"
    (geo, vehicle, energy_type, year, energy_value)
SELECT
    geo,
    vehicle,
    mot_nrg             AS energy_type,
    NULLIF(TRIM("TIME_PERIOD"::TEXT),'')::FLOAT::INT  AS year,
    NULLIF(TRIM(obs_value::TEXT),'')::FLOAT AS energy_value
FROM "tpre612_dataset_clean"."train_traffic_source_energy"
WHERE obs_value IS NOT NULL;


-- -------------------------------------------------------
-- 7. fact_trajet_train
--    Joins: trips → dim_train, dim_route, dim_operateur
--           view_ontd_details → dim_date
--           gares_europeennes  → dim_gare (depart + arrivee)
--           passenger_transported → passengers
--           dim_energie → energy proxy (same geo/year)
-- -------------------------------------------------------
INSERT INTO "tpre612_data_warehouse"."fact_trajet_train"
    (train_id, route_id, operator_id,
     gare_depart_id, gare_arrivee_id,
     date_id,
     distance_km, duree_minutes, emissions_co2,
     passengers, average_speed)

SELECT
    t.trip_id                       AS train_id,
    NULLIF(TRIM(t.route_id::TEXT),'')::FLOAT::INT                 AS route_id,
    t.agency_id                     AS operator_id,

    -- Departure station: match on stop name → gare_id (SERIAL)
    dg_dep.gare_id                  AS gare_depart_id,
    -- Arrival station
    dg_arr.gare_id                  AS gare_arrivee_id,

    -- Date dimension: match on start_date + days-of-week from view_ontd_details
    dd.date_id,

    -- Measures
    NULLIF(TRIM(t.distance::TEXT),'')::FLOAT AS distance_km,
    EXTRACT(EPOCH FROM NULLIF(TRIM(t.duration::TEXT),'')::INTERVAL) / 60.0 AS duree_minutes,

    NULLIF(TRIM(t.emissions_co2e::TEXT),'')::FLOAT         AS emissions_co2,

    -- Passengers from Eurostat (country-level annual, joined via route countries + year)
    NULLIF(TRIM(pt.obs_value::TEXT),'')::FLOAT             AS passengers,

    -- Average speed from view_ontd_details
    NULLIF(TRIM(vod.average_speed::TEXT),'')::FLOAT        AS average_speed

FROM "tpre612_dataset_clean"."trips" t

-- ---- route for country/origin/dest info ----
JOIN "tpre612_data_warehouse"."dim_route" dr
    ON dr.route_id = NULLIF(TRIM(t.route_id::TEXT),'')::FLOAT::INT

-- ---- operator ----
JOIN "tpre612_data_warehouse"."dim_operateur" dop
    ON dop.agency_id = t.agency_id

-- ---- dim_gare: departure (match trip origin to station name) ----
LEFT JOIN "tpre612_data_warehouse"."dim_gare" dg_dep
    ON dg_dep.name    = t.trip_origin
    AND dg_dep.country = SPLIT_PART(t.countries, ',', 1)

-- ---- dim_gare: arrival (match trip headsign / destination) ----
LEFT JOIN "tpre612_data_warehouse"."dim_gare" dg_arr
    ON dg_arr.name    = t.trip_headsign
    AND dg_arr.country = SPLIT_PART(t.countries, ',', -1)

-- ---- view_ontd_details for schedule + speed ----
LEFT JOIN "tpre612_dataset_clean"."view_ontd_details" vod
    ON vod.route_id = NULLIF(TRIM(t.route_id::TEXT),'')::FLOAT::INT

-- ---- dim_date via view_ontd_details service periods ----
LEFT JOIN "tpre612_data_warehouse"."dim_date" dd
    ON dd.start_date = NULLIF(TRIM(vod.start_date_0::TEXT),'')::DATE
    AND dd.end_date  = NULLIF(TRIM(vod.end_date_0::TEXT),'')::DATE

-- ---- passengers: Eurostat country-level annual ----
-- Use the first country listed on the route, most recent year available
LEFT JOIN (
    SELECT geo, obs_value,
           ROW_NUMBER() OVER (PARTITION BY geo ORDER BY "TIME_PERIOD" DESC) AS rn
    FROM "tpre612_dataset_clean"."passenger_transported"
    WHERE obs_value IS NOT NULL
) pt
    ON pt.geo = TRIM(SPLIT_PART(t.countries, ',', 1))
    AND pt.rn = 1

WHERE t.trip_id  IS NOT NULL
  AND t.route_id IS NOT NULL
  AND t.agency_id IN (SELECT agency_id FROM "tpre612_data_warehouse"."dim_operateur");