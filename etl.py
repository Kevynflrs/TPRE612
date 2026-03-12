"""
ETL: tpre612_dataset_clean → tpre612_data_warehouse
- Upserts all dimension and fact tables
- Automatically adds any new columns found in the source that are missing in the target
- Safe to re-run: idempotent (CREATE IF NOT EXISTS + ON CONFLICT DO UPDATE)
"""

import os
import re
import logging
from sqlalchemy import create_engine, text, inspect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Connection ────────────────────────────────────────────────────────────────
DB_URL = "postgresql://postgres:1234@localhost:5432/TPRE612"
engine = create_engine(DB_URL)

SRC  = "tpre612_dataset_clean"
DWH  = "tpre612_data_warehouse"


# ── Helpers ───────────────────────────────────────────────────────────────────

def ensure_schema(conn):
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{DWH}"'))
    log.info(f"Schema {DWH} ready.")


def existing_columns(conn, schema, table):
    """Return set of column names currently in target table (empty set if table doesn't exist)."""
    result = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
    """), {"schema": schema, "table": table})
    return {row[0] for row in result}


def add_missing_columns(conn, schema, table, source_schema, source_table, extra_cast=None):
    """
    Compare source columns to target columns and ALTER TABLE ADD COLUMN for any that are missing.
    New columns are added as TEXT by default (safe fallback); pass extra_cast dict to override.
    extra_cast = {"col_name": "FLOAT", ...}
    """
    extra_cast = extra_cast or {}
    src_cols = existing_columns(conn, source_schema, source_table)
    tgt_cols = existing_columns(conn, schema, table)
    new_cols  = src_cols - tgt_cols

    for col in sorted(new_cols):
        pg_type = extra_cast.get(col, "TEXT")
        safe_col = col.replace('"', '""')
        conn.execute(text(
            f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{safe_col}" {pg_type}'
        ))
        log.info(f"  + Added column '{col}' ({pg_type}) to {schema}.{table}")

    if not new_cols:
        log.info(f"  No new columns for {schema}.{table}")


def safe_float_int(expr):
    """Wrap an expression in a safe TEXT→FLOAT→INT cast."""
    return f"NULLIF(TRIM(({expr})::TEXT),'')::FLOAT::INT"


def safe_float(expr):
    return f"NULLIF(TRIM(({expr})::TEXT),'')::FLOAT"


def safe_date(expr):
    return f"NULLIF(TRIM(({expr})::TEXT),'')::DATE"


def safe_time(expr):
    return f"NULLIF(TRIM({expr}),'')::TIME"



def dedup_table(conn, schema, table, key_cols, fk_refs=None):
    """
    Remove duplicate rows keeping the lowest serial PK per natural key.
    fk_refs: list of (fk_table, fk_col, pk_col) tuples to remap before deleting.
    """
    key_expr = ", ".join(key_cols)
    pk = "gare_id" if table == "dim_gare" else "date_id" if table == "dim_date" else "energy_id"

    # Remap any FK references before deleting
    for (fk_table, fk_col) in (fk_refs or []):
        conn.execute(text(f"""
            UPDATE "{schema}"."{fk_table}" f
            SET "{fk_col}" = sub.keep_id
            FROM (
                SELECT {pk} AS old_id,
                       MIN({pk}) OVER (PARTITION BY {key_expr}) AS keep_id
                FROM "{schema}"."{table}"
                WHERE {" AND ".join(f"{c} IS NOT NULL" for c in key_cols)}
            ) sub
            WHERE f."{fk_col}" = sub.old_id
              AND sub.old_id <> sub.keep_id
        """))

    conn.execute(text(f"""
        DELETE FROM "{schema}"."{table}"
        WHERE {pk} IN (
            SELECT {pk}
            FROM (
                SELECT {pk},
                       ROW_NUMBER() OVER (
                           PARTITION BY {key_expr} ORDER BY {pk}
                       ) AS rn
                FROM "{schema}"."{table}"
                WHERE {" AND ".join(f"{c} IS NOT NULL" for c in key_cols)}
            ) ranked
            WHERE rn > 1
        )
    """))
    log.info(f"  {table} deduplicated on ({key_expr}).")

def safe_interval(expr):
    return f"NULLIF(TRIM(({expr})::TEXT),'')::INTERVAL"


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL = f"""
CREATE TABLE IF NOT EXISTS "{DWH}"."dim_operateur" (
    agency_id       TEXT PRIMARY KEY,
    agency_name     TEXT,
    agency_url      TEXT,
    agency_timezone TEXT,
    agency_lang     TEXT
);

CREATE TABLE IF NOT EXISTS "{DWH}"."dim_route" (
    route_id        INT PRIMARY KEY,
    agency_id       TEXT REFERENCES "{DWH}"."dim_operateur"(agency_id),
    route_long_name TEXT,
    origin          TEXT,
    destination     TEXT,
    countries       TEXT
);

CREATE TABLE IF NOT EXISTS "{DWH}"."dim_train" (
    trip_id                  TEXT PRIMARY KEY,
    route_id                 INT REFERENCES "{DWH}"."dim_route"(route_id),
    trip_headsign            TEXT,
    trip_origin              TEXT,
    destination_arrival_time TIME,
    duration                 INTERVAL,
    distance                 FLOAT
);

CREATE TABLE IF NOT EXISTS "{DWH}"."dim_gare" (
    gare_id         SERIAL PRIMARY KEY,
    name            TEXT,
    city            TEXT,
    country         TEXT,
    latitude        FLOAT,
    longitude       FLOAT,
    is_main_station BOOLEAN
);

CREATE TABLE IF NOT EXISTS "{DWH}"."dim_date" (
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

CREATE TABLE IF NOT EXISTS "{DWH}"."dim_energie" (
    energy_id    SERIAL PRIMARY KEY,
    geo          TEXT,
    vehicle      TEXT,
    energy_type  TEXT,
    year         INT,
    energy_value FLOAT
);

CREATE TABLE IF NOT EXISTS "{DWH}"."fact_trajet_train" (
    fact_id           SERIAL PRIMARY KEY,
    train_id          TEXT  REFERENCES "{DWH}"."dim_train"(trip_id),
    route_id          INT   REFERENCES "{DWH}"."dim_route"(route_id),
    operator_id       TEXT  REFERENCES "{DWH}"."dim_operateur"(agency_id),
    gare_depart_id    INT   REFERENCES "{DWH}"."dim_gare"(gare_id),
    gare_arrivee_id   INT   REFERENCES "{DWH}"."dim_gare"(gare_id),
    date_id           INT   REFERENCES "{DWH}"."dim_date"(date_id),
    distance_km       FLOAT,
    duree_minutes     FLOAT,
    emissions_co2     FLOAT,
    passengers        FLOAT,
    average_speed     FLOAT
);
"""


# ── ETL steps ─────────────────────────────────────────────────────────────────

def step1_dim_operateur(conn):
    log.info("Step 1: dim_operateur")
    add_missing_columns(conn, DWH, "dim_operateur", SRC, "agencies")
    conn.execute(text(f"""
        INSERT INTO "{DWH}"."dim_operateur"
            (agency_id, agency_name, agency_url, agency_timezone, agency_lang)
        SELECT DISTINCT ON (agency_id)
            agency_id, agency_name, agency_url, agency_timezone, agency_lang
        FROM "{SRC}"."agencies"
        WHERE agency_id IS NOT NULL
        ON CONFLICT (agency_id) DO UPDATE SET
            agency_name     = EXCLUDED.agency_name,
            agency_url      = EXCLUDED.agency_url,
            agency_timezone = EXCLUDED.agency_timezone,
            agency_lang     = EXCLUDED.agency_lang
    """))
    log.info("  dim_operateur upserted.")


def step2_dim_route(conn):
    log.info("Step 2: dim_route")
    add_missing_columns(conn, DWH, "dim_route", SRC, "routes", extra_cast={
        "distance": "FLOAT", "emissions": "FLOAT"
    })
    conn.execute(text(f"""
        INSERT INTO "{DWH}"."dim_route"
            (route_id, agency_id, route_long_name, origin, destination, countries)
        SELECT DISTINCT ON (route_id)
            {safe_float_int('r.route_id')},
            r.agency_id,
            r.route_long_name,
            r.origin_trip_0,
            r.destination_trip_0,
            r.countries
        FROM "{SRC}"."routes" r
        WHERE r.route_id IS NOT NULL
          AND r.agency_id IN (SELECT agency_id FROM "{DWH}"."dim_operateur")
        ON CONFLICT (route_id) DO UPDATE SET
            agency_id       = EXCLUDED.agency_id,
            route_long_name = EXCLUDED.route_long_name,
            origin          = EXCLUDED.origin,
            destination     = EXCLUDED.destination,
            countries       = EXCLUDED.countries
    """))
    log.info("  dim_route upserted.")


def step3_dim_train(conn):
    log.info("Step 3: dim_train")
    add_missing_columns(conn, DWH, "dim_train", SRC, "trips", extra_cast={
        "emissions_co2e": "FLOAT", "co2_per_km": "FLOAT",
        "connections": "INT", "catering": "INT",
        "plugs": "INT", "wheelchair_accessible": "INT",
        "bikes_allowed": "INT", "car_transport": "INT",
    })
    conn.execute(text(f"""
        INSERT INTO "{DWH}"."dim_train"
            (trip_id, route_id, trip_headsign, trip_origin,
             destination_arrival_time, duration, distance)
        SELECT DISTINCT ON (t.trip_id)
            t.trip_id,
            {safe_float_int('t.route_id')},
            t.trip_headsign,
            t.trip_origin,
            CASE WHEN t.destination_arrival_time ~ '^\\d{{2}}:\\d{{2}}(:\\d{{2}})?$'
                 THEN {safe_time('t.destination_arrival_time')}
                 ELSE NULL END,
            {safe_interval('t.duration::TEXT')},
            {safe_float('t.distance')}
        FROM "{SRC}"."trips" t
        WHERE t.trip_id IS NOT NULL
          AND {safe_float_int('t.route_id')} IN (
              SELECT route_id FROM "{DWH}"."dim_route"
          )
        ON CONFLICT (trip_id) DO UPDATE SET
            route_id                 = EXCLUDED.route_id,
            trip_headsign            = EXCLUDED.trip_headsign,
            trip_origin              = EXCLUDED.trip_origin,
            destination_arrival_time = EXCLUDED.destination_arrival_time,
            duration                 = EXCLUDED.duration,
            distance                 = EXCLUDED.distance
    """))
    log.info("  dim_train upserted.")


def step4_dim_gare(conn):
    log.info("Step 4: dim_gare")

    # Deduplicate existing rows before creating the unique index.
    # Keep the lowest gare_id per (name, country); update FK references first.
    conn.execute(text(f"""
        WITH keepers AS (
            SELECT MIN(gare_id) AS keep_id, gare_id AS old_id
            FROM "{DWH}"."dim_gare"
            WHERE name IS NOT NULL AND country IS NOT NULL
            GROUP BY name, country, gare_id
        ),
        mapping AS (
            SELECT old_id,
                   MIN(keep_id) OVER (
                       PARTITION BY (
                           SELECT name FROM "{DWH}"."dim_gare" g2 WHERE g2.gare_id = keepers.old_id
                       )
                   ) AS canonical_id
            FROM keepers
        )
        UPDATE "{DWH}"."fact_trajet_train" f
        SET gare_depart_id = m.canonical_id
        FROM mapping m
        WHERE f.gare_depart_id = m.old_id AND m.old_id <> m.canonical_id
    """))
    conn.execute(text(f"""
        WITH dup_ids AS (
            SELECT gare_id
            FROM (
                SELECT gare_id,
                       ROW_NUMBER() OVER (PARTITION BY name, country ORDER BY gare_id) AS rn
                FROM "{DWH}"."dim_gare"
                WHERE name IS NOT NULL AND country IS NOT NULL
            ) ranked
            WHERE rn > 1
        )
        UPDATE "{DWH}"."fact_trajet_train"
        SET gare_arrivee_id = sub.keep_id
        FROM (
            SELECT d.gare_id AS dup_id,
                   MIN(g2.gare_id) AS keep_id
            FROM dup_ids d
            JOIN "{DWH}"."dim_gare" g1 ON g1.gare_id = d.gare_id
            JOIN "{DWH}"."dim_gare" g2 ON g2.name = g1.name AND g2.country = g1.country
            GROUP BY d.gare_id
        ) sub
        WHERE gare_arrivee_id = sub.dup_id
    """))
    conn.execute(text(f"""
        DELETE FROM "{DWH}"."dim_gare"
        WHERE gare_id IN (
            SELECT gare_id
            FROM (
                SELECT gare_id,
                       ROW_NUMBER() OVER (PARTITION BY name, country ORDER BY gare_id) AS rn
                FROM "{DWH}"."dim_gare"
                WHERE name IS NOT NULL AND country IS NOT NULL
            ) ranked
            WHERE rn > 1
        )
    """))
    log.info("  dim_gare deduplicated.")

    # Now safe to create the unique index
    conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS dim_gare_name_country_idx
        ON "{DWH}"."dim_gare" (name, country)
    """))
    add_missing_columns(conn, DWH, "dim_gare", SRC, "gares_europeennes", extra_cast={
        "uic": "FLOAT", "uic8_sncf": "FLOAT",
        "parent_station_id": "FLOAT",
    })

    # 4a — gares_europeennes
    conn.execute(text(f"""
        INSERT INTO "{DWH}"."dim_gare"
            (name, city, country, latitude, longitude, is_main_station)
        SELECT DISTINCT ON (g.name, g.country)
            g.name,
            g.name,
            g.country,
            {safe_float('g.latitude')},
            {safe_float('g.longitude')},
            (g.is_main_station = 't')
        FROM "{SRC}"."gares_europeennes" g
        WHERE g.latitude IS NOT NULL AND g.longitude IS NOT NULL AND g.name IS NOT NULL
        ON CONFLICT (name, country) DO UPDATE SET
            latitude        = EXCLUDED.latitude,
            longitude       = EXCLUDED.longitude,
            is_main_station = EXCLUDED.is_main_station
    """))

    # 4b — night-train stops
    conn.execute(text(f"""
        INSERT INTO "{DWH}"."dim_gare"
            (name, city, country, latitude, longitude, is_main_station)
        SELECT DISTINCT ON (s.stop_name, s.stop_country)
            s.stop_name,
            COALESCE(s.stop_cityname, s.stop_name),
            s.stop_country,
            {safe_float('s.stop_lat')},
            {safe_float('s.stop_lon')},
            FALSE
        FROM "{SRC}"."stops" s
        WHERE s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL AND s.stop_name IS NOT NULL
        ON CONFLICT (name, country) DO UPDATE SET
            city      = EXCLUDED.city,
            latitude  = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
    """))
    log.info("  dim_gare upserted.")


def step5_dim_date(conn):
    log.info("Step 5: dim_date")
    dedup_table(conn, DWH, "dim_date", ["start_date", "end_date"],
                fk_refs=[("fact_trajet_train", "date_id")])
    conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS dim_date_range_idx
        ON "{DWH}"."dim_date" (start_date, end_date)
    """))

    def day_cast(col):
        return f"(NULLIF(TRIM(({col})::TEXT),'')::FLOAT::INT = 1)"

    for sfx in ["0", "1"]:
        conn.execute(text(f"""
            INSERT INTO "{DWH}"."dim_date"
                (start_date, end_date,
                 monday, tuesday, wednesday, thursday, friday, saturday, sunday)
            SELECT DISTINCT ON (start_date, end_date)
                {safe_date(f'start_date_{sfx}')} AS start_date,
                {safe_date(f'end_date_{sfx}')}   AS end_date,
                {day_cast(f'monday_{sfx}')},
                {day_cast(f'tuesday_{sfx}')},
                {day_cast(f'wednesday_{sfx}')},
                {day_cast(f'thursday_{sfx}')},
                {day_cast(f'friday_{sfx}')},
                {day_cast(f'saturday_{sfx}')},
                {day_cast(f'sunday_{sfx}')}
            FROM "{SRC}"."view_ontd_details"
            WHERE start_date_{sfx} IS NOT NULL AND end_date_{sfx} IS NOT NULL
            ORDER BY start_date, end_date
            ON CONFLICT (start_date, end_date) DO UPDATE SET
                monday    = EXCLUDED.monday,
                tuesday   = EXCLUDED.tuesday,
                wednesday = EXCLUDED.wednesday,
                thursday  = EXCLUDED.thursday,
                friday    = EXCLUDED.friday,
                saturday  = EXCLUDED.saturday,
                sunday    = EXCLUDED.sunday
        """))
    log.info("  dim_date upserted.")


def step6_dim_energie(conn):
    log.info("Step 6: dim_energie")
    dedup_table(conn, DWH, "dim_energie",["geo", "vehicle", "energy_type", "year"])
    
    conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS dim_energie_natural_idx
        ON "{DWH}"."dim_energie" (geo, vehicle, energy_type, year)
    """))
    
    add_missing_columns(conn, DWH, "dim_energie", SRC, "train_traffic_source_energy", extra_cast={
        "train": "TEXT",
    })
    
    conn.execute(text(f"""
        WITH deduped_source AS (
            SELECT DISTINCT ON (
                geo,
                vehicle,
                mot_nrg,
                {safe_float_int('"TIME_PERIOD"')}
            )
                geo,
                vehicle,
                mot_nrg AS energy_type,
                {safe_float_int('"TIME_PERIOD"')} AS year,
                {safe_float('obs_value')} AS energy_value
            FROM "{SRC}"."train_traffic_source_energy"
            WHERE obs_value IS NOT NULL
        )
        INSERT INTO "{DWH}"."dim_energie"
            (geo, vehicle, energy_type, year, energy_value)
        SELECT
            geo,
            vehicle,
            energy_type,
            year,
            energy_value
        FROM deduped_source
        ON CONFLICT (geo, vehicle, energy_type, year) DO UPDATE SET
            energy_value = EXCLUDED.energy_value
    """))
    log.info("  dim_energie upserted.")


def step7_fact_trajet_train(conn):
    log.info("Step 7: fact_trajet_train")
    
    # ADDED: Force add the columns that come from JOINs in case the table is from an older run
    conn.execute(text(f'ALTER TABLE "{DWH}"."fact_trajet_train" ADD COLUMN IF NOT EXISTS passengers FLOAT;'))
    conn.execute(text(f'ALTER TABLE "{DWH}"."fact_trajet_train" ADD COLUMN IF NOT EXISTS average_speed FLOAT;'))

    add_missing_columns(conn, DWH, "fact_trajet_train", SRC, "trips", extra_cast={
        "via": "TEXT", "irregularities": "TEXT",
        "service_id": "TEXT", "classes": "TEXT",
        "countries": "TEXT", "is_active": "TEXT",
        "direction_id": "INT", "version": "TEXT",
    })
    
    conn.execute(text(f"""
        INSERT INTO "{DWH}"."fact_trajet_train"
            (train_id, route_id, operator_id,
             gare_depart_id, gare_arrivee_id,
             date_id,
             distance_km, duree_minutes, emissions_co2,
             passengers, average_speed)
        SELECT
            t.trip_id,
            {safe_float_int('t.route_id')},
            t.agency_id,
            dg_dep.gare_id,
            dg_arr.gare_id,
            dd.date_id,
            {safe_float('t.distance')},
            EXTRACT(EPOCH FROM {safe_interval('t.duration::TEXT')}) / 60.0,
            {safe_float('t.emissions_co2e')},
            {safe_float('pt.obs_value')},
            {safe_float('vod.average_speed')}
        FROM "{SRC}"."trips" t
        JOIN "{DWH}"."dim_route" dr
            ON dr.route_id = {safe_float_int('t.route_id')}
        JOIN "{DWH}"."dim_operateur" dop
            ON dop.agency_id = t.agency_id
        LEFT JOIN "{DWH}"."dim_gare" dg_dep
            ON dg_dep.name    = t.trip_origin
           AND dg_dep.country = TRIM(SPLIT_PART(t.countries, ',', 1))
        LEFT JOIN "{DWH}"."dim_gare" dg_arr
            ON dg_arr.name    = t.trip_headsign
           AND dg_arr.country = TRIM(SPLIT_PART(t.countries, ',', -1))
        LEFT JOIN "{SRC}"."view_ontd_details" vod
            ON vod.route_id = {safe_float_int('t.route_id')}
        LEFT JOIN "{DWH}"."dim_date" dd
            ON dd.start_date = {safe_date('vod.start_date_0')}
           AND dd.end_date   = {safe_date('vod.end_date_0')}
        LEFT JOIN (
            SELECT geo, obs_value,
                   ROW_NUMBER() OVER (PARTITION BY geo ORDER BY "TIME_PERIOD" DESC) AS rn
            FROM "{SRC}"."passenger_transported"
            WHERE obs_value IS NOT NULL
        ) pt
            ON pt.geo = TRIM(SPLIT_PART(t.countries, ',', 1))
           AND pt.rn  = 1
        WHERE t.trip_id  IS NOT NULL
          AND t.route_id IS NOT NULL
          AND t.agency_id IN (SELECT agency_id FROM "{DWH}"."dim_operateur")
        ON CONFLICT DO NOTHING
    """))
    log.info("  fact_trajet_train upserted.")

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    with engine.begin() as conn:
        ensure_schema(conn)

        # Create tables (idempotent)
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))

        step1_dim_operateur(conn)
        step2_dim_route(conn)
        step3_dim_train(conn)
        step4_dim_gare(conn)
        step5_dim_date(conn)
        step6_dim_energie(conn)
        step7_fact_trajet_train(conn)

    log.info("✓ ETL complete.")


if __name__ == "__main__":
    run()