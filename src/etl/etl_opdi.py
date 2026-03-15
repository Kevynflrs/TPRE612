import argparse
import logging
from datetime import date, timedelta
from io import BytesIO
import pandas as pd
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

BASE_URL = "https://www.eurocontrol.int/performance/data/download/OPDI/v002"

EUROPEAN_ICAO_PREFIXES = (
    "EB", "ED", "ET", "EE", "EF", "EG", "EH", "EI", "EK", "EL",
    "EN", "EP", "ES", "EV", "EY", "GC", "GE", "LA", "LB", "LC",
    "LD", "LE", "LF", "LG", "LH", "LI", "LJ", "LK", "LM", "LN",
    "LO", "LP", "LQ", "LR", "LS", "LT", "LU", "LW", "LX", "LY",
    "LZ", "UB", "UD", "UG", "UK", "UM",
)


#CO2 (kg) = distance_nm * factor
CO2_KG_PER_NM = {
    "L2J": 24.0,
    "L4J": 48.0,
    "L1J": 8.0,
    "L3J": 36.0,
    "L2T": 10.0,
    "L1T": 5.0,
    "L4T": 20.0,
    "L2P": 4.0,
    "L1P": 2.0,
    "H1T": 6.0,
    "H2T": 10.0,
    "H1P": 3.0,
}
DEFAULT_CO2_KG_PER_NM = 24.0


# Helpers

def _monthly_periods(start: str, end: str) -> list[str]:
    p = pd.Period(start, freq="M")
    e = pd.Period(end, freq="M")
    result = []
    while p <= e:
        result.append(p.strftime("%Y%m"))
        p += 1
    return result


def _ten_day_periods(start: str, end: str) -> list[tuple[str, str]]:
    d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    result = []
    while d <= end_d:
        d_next = d + timedelta(days=10)
        result.append((d.strftime("%Y%m%d"), d_next.strftime("%Y%m%d")))
        d = d_next
    return result


def _fetch_parquet(url: str) -> pd.DataFrame | None:
    try:
        r = requests.get(url, timeout=120)
        if r.status_code == 404:
            logger.debug("404 not found: %s", url)
            return None
        r.raise_for_status()
        return pd.read_parquet(BytesIO(r.content))
    except Exception as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        return None


# Extract

def _extract_flight_list(months: list[str]) -> pd.DataFrame:
    frames = []
    for ym in tqdm(months, desc="[OPDI] flight_list"):
        df = _fetch_parquet(f"{BASE_URL}/flight_list/flight_list_{ym}.parquet")
        if df is not None:
            frames.append(df)
    if not frames:
        raise RuntimeError("[OPDI] No flight_list files downloaded.")
    return pd.concat(frames, ignore_index=True)


def _extract_measurements(periods: list[tuple[str, str]]) -> pd.DataFrame:
    frames = []
    for s, e in tqdm(periods, desc="[OPDI] measurements"):
        df = _fetch_parquet(f"{BASE_URL}/measurements/measurements_{s}_{e}.parquet")
        if df is not None:
            frames.append(df)
    if not frames:
        raise RuntimeError("[OPDI] No measurements files downloaded.")
    return pd.concat(frames, ignore_index=True)


def _extract_flight_events(periods: list[tuple[str, str]]) -> pd.DataFrame:
    frames = []
    for s, e in tqdm(periods, desc="[OPDI] flight_events"):
        df = _fetch_parquet(f"{BASE_URL}/flight_events/flight_events_{s}_{e}.parquet")
        if df is not None:
            frames.append(df)
    if not frames:
        raise RuntimeError("[OPDI] No flight_events files downloaded.")
    return pd.concat(frames, ignore_index=True)


# Transform

def _is_european(series: pd.Series) -> pd.Series:
    return series.str.upper().str.startswith(EUROPEAN_ICAO_PREFIXES)


def _transform_flights(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the flight list, drops rows without ADEP/ADES,
    and keeps only intra-European flights.
    """
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    df = df.rename(columns={
        "id": "flight_id",
        "adep": "airport_depart_id",
        "ades": "airport_arrivee_id",
        "dof": "date_id",
        "model": "modele_avion",
        "typecode": "plane_id",
        "icao_operator": "operator_id",
        "icao_aircraft_class": "icao_aircraft_class",
    })

    logger.info("[OPDI] Raw flights: %s", f"{len(df):,}")

    df = df.dropna(subset=["airport_depart_id", "airport_arrivee_id"])
    logger.info("[OPDI] After dropping missing ADEP/ADES: %s", f"{len(df):,}")

    mask = _is_european(df["airport_depart_id"]) & _is_european(df["airport_arrivee_id"])
    df = df[mask].copy()
    logger.info("[OPDI] Intra-European flights: %s", f"{len(df):,}")

    df["date_id"] = pd.to_datetime(df["date_id"], errors="coerce")
    df["flight_id"] = df["flight_id"].astype("uint64")

    keep = [
        "flight_id", "plane_id", "operator_id",
        "airport_depart_id", "airport_arrivee_id", "date_id",
        "modele_avion", "icao_aircraft_class",
    ]
    return df[[c for c in keep if c in df.columns]].drop_duplicates(subset=["flight_id"])


def _compute_timestamps(df_events_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Computes departure and arrival timestamps per flight from flight_events.

    heure_depart  = min(event_time) for that flight
    heure_arrivee = max(event_time) for that flight
    """
    df = df_events_raw.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    time_col = next((c for c in ["event_time", "time", "timestamp"] if c in df.columns), None)
    if not time_col or "flight_id" not in df.columns:
        logger.warning("[OPDI] Missing columns in flight_events. Available: %s", list(df.columns))
        return pd.DataFrame(columns=["flight_id", "heure_depart", "heure_arrivee"])

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col, "flight_id"])
    df["flight_id"] = df["flight_id"].astype("uint64")

    agg = df.groupby("flight_id")[time_col].agg(["min", "max"]).reset_index()
    agg.columns = ["flight_id", "heure_depart", "heure_arrivee"]
    agg = agg[agg["heure_depart"] < agg["heure_arrivee"]]

    logger.info("[OPDI] Timestamps computed for %s flights", f"{len(agg):,}")
    return agg


def _compute_measurements(df_measurements_raw: pd.DataFrame, df_events_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts distance (NM) and duration (minutes) per flight from measurements.

    Join chain: measurements.event_id -> flight_events.id -> flight_events.flight_id
    """
    df = df_measurements_raw.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    ev = df_events_raw.copy()
    ev.columns = [c.strip().lower() for c in ev.columns]

    mapping = (
        ev[["id", "flight_id"]]
        .drop_duplicates(subset=["id"])
        .rename(columns={"id": "event_id"})
    )
    mapping["flight_id"] = mapping["flight_id"].astype("uint64")
    logger.info("[OPDI] event_id to flight_id mapping: %s entries", f"{len(mapping):,}")

    df = df.merge(mapping, on="event_id", how="left")
    df = df.dropna(subset=["flight_id"])
    df["flight_id"] = df["flight_id"].astype("uint64")

    # Distance: cumulative NM, max = total distance flown
    df_dist = (
        df[df["type"] == "Distance flown (NM)"]
        .groupby("flight_id")["value"]
        .max().reset_index()
        .rename(columns={"value": "distance_nm"})
    )
    logger.info("[OPDI] Flights with distance: %s", f"{len(df_dist):,}")

    # Duration: cumulative seconds, max = total flight time
    df_time = (
        df[df["type"] == "Time Passed (s)"]
        .groupby("flight_id")["value"]
        .max().reset_index()
    )
    df_time["duree_minutes"] = (df_time["value"] / 60).round(1)
    df_time = df_time[df_time["duree_minutes"] > 0][["flight_id", "duree_minutes"]]
    logger.info("[OPDI] Flights with duration: %s", f"{len(df_time):,}")

    return df_dist.merge(df_time, on="flight_id", how="outer")


def _build_fact(
    df_flights: pd.DataFrame,
    df_measurements: pd.DataFrame,
    df_timestamps: pd.DataFrame,
) -> pd.DataFrame:
    """
    Joins all transformed tables and returns the final fact dataframe.

    CO2 is estimated here as distance_nm * emission_factor(icao_aircraft_class)
    """
    df = (
        df_flights
        .merge(df_measurements, on="flight_id", how="left")
        .merge(df_timestamps,   on="flight_id", how="left")
    )

    if "distance_nm" in df.columns:
        df["distance_km"] = (df["distance_nm"] * 1.852).round(2)

    if "distance_nm" in df.columns:
        factor = (
            df["icao_aircraft_class"].map(CO2_KG_PER_NM).fillna(DEFAULT_CO2_KG_PER_NM)
            if "icao_aircraft_class" in df.columns
            else DEFAULT_CO2_KG_PER_NM
        )
        df["emissions_co2"] = (df["distance_nm"] * factor).round(2)

    df = df.reset_index(drop=True)
    df.insert(0, "fact_id", df.index + 1)

    final_cols = [
        "fact_id",
        "plane_id",
        "operator_id",
        "airport_depart_id",
        "airport_arrivee_id",
        "date_id",
        "heure_depart",
        "heure_arrivee",
        "distance_km",
        "duree_minutes",
        "emissions_co2",
        "modele_avion",
    ]
    return df[[c for c in final_cols if c in df.columns]]


# Public interface

def get_opdi_data(
    start: str = "2022-01",
    end: str = "2026-01",
    dry_run: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Downloads and transforms OPDI data into a fact table.

    Args:
        start:   First month to download, format YYYY-MM.
        end:     Last month to download, format YYYY-MM.
        dry_run: If True, limits to a single month (2022-01) for testing.

    Returns:
        A dict with one key: "fact_trajet_avion" mapped to a pd.DataFrame.
    """
    if dry_run:
        logger.info("[OPDI] dry-run: limited to 2022-01")
        start, end = "2022-01", "2022-01"

    months  = _monthly_periods(start, end)
    start_d = f"{start[:4]}-{start[5:7]}-01"
    end_d   = (pd.Period(end, freq="M") + 1).start_time.date().isoformat()
    periods = _ten_day_periods(start_d, end_d)

    logger.info("[OPDI] %s months | %s ten-day periods", len(months), len(periods))

    logger.info("[OPDI] Step 1/4 - downloading flight_list (%s months)...", len(months))
    df_flights = _transform_flights(_extract_flight_list(months))

    logger.info("[OPDI] Step 2/4 - downloading measurements and flight_events...")
    df_meas_raw   = _extract_measurements(periods)
    df_events_raw = _extract_flight_events(periods)

    logger.info("[OPDI] Step 3/4 - computing measurements and timestamps...")
    df_measurements = _compute_measurements(df_meas_raw, df_events_raw)
    df_timestamps   = _compute_timestamps(df_events_raw)

    logger.info("[OPDI] Step 4/4 - building final fact table...")
    fact = _build_fact(df_flights, df_measurements, df_timestamps)

    logger.info("[OPDI] Done. fact_trajet_avion: %s rows, %s columns", f"{len(fact):,}", len(fact.columns))
    logger.info("\n%s", fact.head(3).to_string())

    return {"fact_trajet_avion": fact}


# Run
# if __name__ == "__main__":
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s [%(levelname)s] %(message)s",
#         datefmt="%Y-%m-%d %H:%M:%S",
#     )

#     parser = argparse.ArgumentParser(description="ETL OPDI - builds fact_trajet_avion")
#     parser.add_argument("--start",   default="2022-01", help="Start month YYYY-MM")
#     parser.add_argument("--end",     default="2026-01", help="End month YYYY-MM")
#     parser.add_argument("--dry-run", action="store_true", help="Run on a single month only")
#     args = parser.parse_args()

#     result = get_opdi_data(start=args.start, end=args.end, dry_run=args.dry_run)
#     fact = result["fact_trajet_avion"]

#     if not fact.empty:
#         fact.to_csv("fact_trajet_avion.csv", index=False)
#         print(f"\nSaved {len(fact):,} rows")
#         print(fact.head(5).to_string())
#     else:
#         print("DataFrame empty")