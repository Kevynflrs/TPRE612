import pandas as pd


def populate_dim_train(db_clean, db_warehouse):
    trips = db_clean.get_data_from_table("trips")
    print(f"Trips bruts : {len(trips)}")

    valid_routes = set(db_warehouse.get_data_from_table(
        "dim_route")["route_id"].astype(int).tolist())
    print(f"Routes valides : {len(valid_routes)}")

    df = pd.DataFrame({
        "trip_id":                  trips["trip_id"].astype(str).str.strip(),
        "route_id":                 pd.to_numeric(trips["route_id"].astype(str).str.strip(), errors="coerce").astype("Int64"),
        "trip_headsign":            trips["trip_headsign"],
        "trip_origin":              trips["trip_origin"],
        "destination_arrival_time": trips["destination_arrival_time"].apply(_safe_time),
        "duration":                 trips["duration"].apply(_safe_interval),
        "distance":                 pd.to_numeric(trips["distance"].astype(str).str.strip(), errors="coerce"),
        "is_night_train":           _coerce_is_night_train(trips)
    })

    df = (
        df.dropna(subset=["trip_id", "route_id"])
        .loc[lambda x: x["route_id"].astype(int).isin(valid_routes)]
        .drop_duplicates(subset=["trip_id"], keep="first")
        .reset_index(drop=True)
    )
    df["route_id"] = df["route_id"].astype(int)

    print(f"Lignes dim_train à upsert : {len(df)}")
    db_warehouse.upsert(df, "dim_train", conflict_columns=[
                        "trip_id"], schema="tpre612_data_warehouse")
    print("dim_train OK")


def _safe_time(val):
    """Convert string to time if it matches HH:MM or HH:MM:SS, else None."""
    import re
    from datetime import time
    if pd.isna(val):
        return None
    val = str(val).strip()
    if re.match(r'^\d{2}:\d{2}(:\d{2})?$', val):
        parts = val.split(":")
        try:
            h, m, s = int(parts[0]), int(parts[1]), int(
                parts[2]) if len(parts) == 3 else 0
            # PostgreSQL TIME allows hours > 23 for GTFS overnight trips — clamp to None if invalid
            if h > 23:
                return None
            return time(h, m, s)
        except ValueError:
            return None
    return None


def _safe_interval(val):
    """Convert a duration string/number to a pandas Timedelta (maps to INTERVAL)."""
    if pd.isna(val):
        return None
    try:
        return pd.to_timedelta(str(val).strip())
    except Exception:
        return None


def _coerce_is_night_train(df):
    """Return a clean boolean Series; defaults to False when column is missing."""
    if "is_night_train" not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)

    raw = df["is_night_train"]
    if pd.api.types.is_bool_dtype(raw):
        return raw.fillna(False).astype(bool)

    true_values = {"1", "true", "t", "yes", "y", "oui", "vrai"}
    normalized = raw.astype(str).str.strip().str.lower()
    return normalized.isin(true_values)