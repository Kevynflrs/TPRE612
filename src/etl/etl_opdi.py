import argparse
import logging
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
import pandas as pd
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

BASE_URL = "https://www.eurocontrol.int/performance/data/download/OPDI/v002"

DEFAULT_AIRPORTS_CSV = "data/airports.csv"

EUROPEAN_ICAO_PREFIXES = (
    "EB", "ED", "ET", "EE", "EF", "EG", "EH", "EI", "EK", "EL",
    "EN", "EP", "ES", "EV", "EY", "GC", "GE", "LA", "LB", "LC",
    "LD", "LE", "LF", "LG", "LH", "LI", "LJ", "LK", "LM", "LN",
    "LO", "LP", "LQ", "LR", "LS", "LT", "LU", "LW", "LX", "LY",
    "LZ", "UB", "UD", "UG", "UK", "UM",
)

# CO2 (kg) = distance_nm * factor
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


def _ten_day_periods_for_month(ym: str) -> list[tuple[str, str]]:
    """Returns the ten-day periods that cover a given YYYYMM month."""
    p = pd.Period(ym, freq="M")
    start_d = date(p.year, p.month, 1)
    end_d = (p + 1).start_time.date()
    result = []
    d = start_d
    while d < end_d:
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
        if r.content[:4] != b"PAR1":
            logger.warning("Not a valid parquet file: %s", url)
            return None
        return pd.read_parquet(BytesIO(r.content))
    except Exception as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        return None


def _load_airport_names(csv_path: Path) -> dict[str, str]:
    """
    Reads airports.csv and returns a dict mapping ICAO code to city name.
    Falls back to an empty dict if the file is missing or unreadable.
    """
    try:
        df = pd.read_csv(csv_path, usecols=["icao_code", "municipality"], dtype=str)
        df = df.dropna(subset=["icao_code", "municipality"])
        df["icao_code"] = df["icao_code"].str.strip().str.upper()
        df["municipality"] = df["municipality"].str.strip()
        mapping = (
            df.drop_duplicates(subset=["icao_code"])
            .set_index("icao_code")["municipality"]
            .to_dict()
        )
        logger.info("[OPDI] Loaded %s airport names from %s", len(mapping), csv_path)
        return mapping
    except Exception as exc:
        logger.warning("[OPDI] Could not load airports CSV (%s): %s", csv_path, exc)
        return {}



# Per-month processing

def _is_european(series: pd.Series) -> pd.Series:
    return series.str.upper().str.startswith(EUROPEAN_ICAO_PREFIXES)


def _process_month(ym: str, airport_names: dict[str, str]) -> pd.DataFrame:
    """
    Downloads and processes one month of data.
    Returns a partial fact dataframe for that month.
    All three sources are loaded, processed, and discarded within this function.
    """
    logger.info("[OPDI] Processing month %s...", ym)
    periods = _ten_day_periods_for_month(ym)

    # --- flight_list ---
    df_fl = _fetch_parquet(f"{BASE_URL}/flight_list/flight_list_{ym}.parquet")
    if df_fl is None:
        logger.warning("[OPDI] No flight_list for %s, skipping.", ym)
        return pd.DataFrame()

    df_fl.columns = [c.strip().lower() for c in df_fl.columns]
    df_fl = df_fl.rename(columns={
        "id": "flight_id",
        "adep": "airport_depart_id",
        "ades": "airport_arrivee_id",
        "dof": "date_id",
        "model": "modele_avion",
        "typecode": "plane_id",
        "icao_operator": "operator_id",
        "icao_aircraft_class": "icao_aircraft_class",
    })
    df_fl = df_fl.dropna(subset=["airport_depart_id", "airport_arrivee_id"])
    mask = _is_european(df_fl["airport_depart_id"]) & _is_european(df_fl["airport_arrivee_id"])
    df_fl = df_fl[mask].copy()

    if df_fl.empty:
        return pd.DataFrame()

    df_fl["date_id"] = pd.to_datetime(df_fl["date_id"], errors="coerce")
    df_fl["flight_id"] = df_fl["flight_id"].astype("uint64")

    if airport_names:
        df_fl["airport_depart_id"] = df_fl["airport_depart_id"].map(airport_names).fillna(df_fl["airport_depart_id"])
        df_fl["airport_arrivee_id"] = df_fl["airport_arrivee_id"].map(airport_names).fillna(df_fl["airport_arrivee_id"])

    keep = ["flight_id", "plane_id", "operator_id", "airport_depart_id",
            "airport_arrivee_id", "date_id", "modele_avion", "icao_aircraft_class"]
    df_fl = df_fl[[c for c in keep if c in df_fl.columns]].drop_duplicates(subset=["flight_id"])

    # --- measurements + flight_events ---
    meas_frames = []
    ev_frames = []
    for s, e in periods:
        df_m = _fetch_parquet(f"{BASE_URL}/measurements/measurements_{s}_{e}.parquet")
        if df_m is not None:
            meas_frames.append(df_m)
        df_e = _fetch_parquet(f"{BASE_URL}/flight_events/flight_events_{s}_{e}.parquet")
        if df_e is not None:
            ev_frames.append(df_e)

    if not meas_frames:
        logger.warning("[OPDI] No measurements downloaded for %s — skipping.", ym)
        return pd.DataFrame()
    if not ev_frames:
        logger.warning("[OPDI] No flight_events downloaded for %s — skipping.", ym)
        return pd.DataFrame()
    logger.info("[OPDI] Month %s: %s meas files, %s event files", ym, len(meas_frames), len(ev_frames))

    df_meas = pd.concat(meas_frames, ignore_index=True)
    df_ev   = pd.concat(ev_frames, ignore_index=True)
    del meas_frames, ev_frames

    df_meas.columns = [c.strip().lower() for c in df_meas.columns]
    df_ev.columns   = [c.strip().lower() for c in df_ev.columns]

    # Build event_id -> flight_id mapping
    mapping = (
        df_ev[["id", "flight_id"]]
        .drop_duplicates(subset=["id"])
        .rename(columns={"id": "event_id"})
    )
    mapping["flight_id"] = mapping["flight_id"].astype("uint64")

    # Timestamps: first and last event per flight
    time_col = next((c for c in ["event_time", "time", "timestamp"] if c in df_ev.columns), None)
    if time_col:
        df_ev[time_col] = pd.to_datetime(df_ev[time_col], errors="coerce")
        df_ev["flight_id"] = df_ev["flight_id"].astype("uint64")
        ts = df_ev.groupby("flight_id")[time_col].agg(["min", "max"]).reset_index()
        ts.columns = ["flight_id", "heure_depart", "heure_arrivee"]
        ts = ts[ts["heure_depart"] < ts["heure_arrivee"]]
    else:
        ts = pd.DataFrame(columns=["flight_id", "heure_depart", "heure_arrivee"])
    del df_ev

    # Measurements: distance and duration
    df_meas = df_meas.merge(mapping, on="event_id", how="left")
    df_meas = df_meas.dropna(subset=["flight_id"])
    df_meas["flight_id"] = df_meas["flight_id"].astype("uint64")
    del mapping

    df_dist = (
        df_meas[df_meas["type"] == "Distance flown (NM)"]
        .groupby("flight_id")["value"].max().reset_index()
        .rename(columns={"value": "distance_nm"})
    )
    df_time = (
        df_meas[df_meas["type"] == "Time Passed (s)"]
        .groupby("flight_id")["value"].max().reset_index()
    )
    del df_meas

    df_time["duree_minutes"] = (df_time["value"] / 60).round(1)
    df_time = df_time[df_time["duree_minutes"] > 0][["flight_id", "duree_minutes"]]

    # Join
    df = (
        df_fl
        .merge(df_dist, on="flight_id", how="left")
        .merge(df_time, on="flight_id", how="left")
        .merge(ts,      on="flight_id", how="left")
    )
    del df_fl, df_dist, df_time, ts

    # Compute distance_km and emissions_co2
    if "distance_nm" in df.columns:
        df["distance_km"] = (df["distance_nm"] * 1.852).round(2)
        factor = (
            df["icao_aircraft_class"].map(CO2_KG_PER_NM).fillna(DEFAULT_CO2_KG_PER_NM)
            if "icao_aircraft_class" in df.columns
            else DEFAULT_CO2_KG_PER_NM
        )
        df["emissions_co2"] = (df["distance_nm"] * factor).round(2)

    # Drop rows missing the two columns that cannot be estimated
    df = df.dropna(subset=["distance_km", "emissions_co2"])

    # Sample down to 8500 rows per month
    if len(df) > 8500:
        df = df.sample(n=8500, random_state=42)

    final_cols = [
        "flight_id", "plane_id", "operator_id",
        "airport_depart_id", "airport_arrivee_id", "date_id",
        "heure_depart", "heure_arrivee",
        "distance_km", "duree_minutes", "emissions_co2", "modele_avion",
    ]
    result = df[[c for c in final_cols if c in df.columns]]
    logger.info("[OPDI] Month %s: %s rows after filtering and sampling", ym, f"{len(result):,}")
    return result



# Public interface

def get_opdi_data(
    start: str = "2022-01",
    end: str = "2026-01",
    dry_run: bool = False,
    airports_csv: Path | str = DEFAULT_AIRPORTS_CSV,
) -> dict[str, pd.DataFrame]:
    """
    Downloads and transforms OPDI data into a fact table.

    Args:
        start:        First month to download, format YYYY-MM.
        end:          Last month to download, format YYYY-MM.
        dry_run:      If True, limits to a single month (2022-01) for testing.
        airports_csv: Path to airports.csv (OurAirports format).

    Returns:
        A dict with one key: "fact_trajet_avion" mapped to a pd.DataFrame.
    """
    if dry_run:
        logger.info("[OPDI] dry-run: limited to 2022-01")
        start, end = "2022-01", "2022-01"

    airport_names = _load_airport_names(Path(airports_csv))
    months = _monthly_periods(start, end)
    logger.info("[OPDI] Processing %s months one at a time...", len(months))

    chunks = []
    for ym in tqdm(months, desc="[OPDI] months"):
        chunk = _process_month(ym, airport_names)
        if not chunk.empty:
            chunks.append(chunk)

    if not chunks:
        logger.error("[OPDI] No data collected.")
        return {"fact_trajet_avion": pd.DataFrame()}

    fact = pd.concat(chunks, ignore_index=True)
    fact = fact.reset_index(drop=True)
    fact.insert(0, "fact_id", fact.index + 1)

    final_cols = [
        "fact_id", "plane_id", "operator_id",
        "airport_depart_id", "airport_arrivee_id", "date_id",
        "heure_depart", "heure_arrivee",
        "distance_km", "duree_minutes", "emissions_co2", "modele_avion",
    ]
    fact = fact[[c for c in final_cols if c in fact.columns]]

    logger.info("[OPDI] Done. fact_trajet_avion: %s rows, %s columns", f"{len(fact):,}", len(fact.columns))
    logger.info("\n%s", fact.head(3).to_string())

    return {"fact_trajet_avion": fact}



# Run directly

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="ETL OPDI - builds fact_trajet_avion")
    parser.add_argument("--start", default="2022-01", help="Start month YYYY-MM")
    parser.add_argument("--end", default="2026-01", help="End month YYYY-MM")
    parser.add_argument("--dry-run", action="store_true", help="Run on a single month only")
    parser.add_argument("--airports-csv", default=str(DEFAULT_AIRPORTS_CSV), help="Path to airports.csv")
    args = parser.parse_args()

    result = get_opdi_data(
        start=args.start,
        end=args.end,
        dry_run=args.dry_run,
        airports_csv=args.airports_csv,
    )
    fact = result["fact_trajet_avion"]

    if not fact.empty:
        fact.to_csv("fact_trajet_avion.csv", index=False)
        print(f"\nSaved {len(fact):,} rows")
        print(fact.head(5).to_string())
    else:
        print("DataFrame empty")