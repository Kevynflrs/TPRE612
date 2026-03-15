import logging
import math
import hashlib
import requests
import pandas as pd
from io import StringIO
from typing import Dict, Optional
from datetime import date

logger = logging.getLogger(__name__)

CSV_URL = (
    "https://mobilithek.info//mdp-api/files/aux/"
    "632998123011989504/Intermodal_connections_Europe.csv"
)
SNAPSHOT_DATE = date(2023, 8, 1)
SNAPSHOT_END  = date(2023, 12, 31)


def _haversine_km(lat1, lon1, lat2, lon2) -> Optional[float]:
    try:
        R = 6371.0
        phi1, phi2 = math.radians(float(lat1)), math.radians(float(lat2))
        dphi    = math.radians(float(lat2) - float(lat1))
        dlambda = math.radians(float(lon2) - float(lon1))
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)
    except Exception:
        return None


def _stable_id(value: str, prefix: str = "") -> str:
    h = hashlib.md5(str(value).encode("utf-8")).hexdigest()[:8]
    return f"{prefix}{h}"


def _parse_coords(coord_str: str) -> tuple:
    """Parse 'lat, lon' -> (float, float) ou (None, None)."""
    try:
        parts = str(coord_str).split(",")
        if len(parts) >= 2:
            return float(parts[0].strip()), float(parts[1].strip())
    except Exception:
        pass
    return None, None


def _load_csv(url: str, local_csv=None) -> pd.DataFrame:
    import os
    if local_csv and os.path.exists(local_csv):
        logger.info(f"Lecture CSV local : {local_csv}")
        for sep in [";", ",", "\t"]:
            df = pd.read_csv(local_csv, sep=sep, dtype=str, low_memory=False)
            if len(df.columns) > 3:
                logger.info(f"CSV parsé sep='{sep}' - {len(df)} lignes, {len(df.columns)} colonnes")
                return df
    else:
        if local_csv:
            logger.warning(f"Fichier local '{local_csv}' introuvable, téléchargement automatique.")
        logger.info(f"Téléchargement : {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        for sep in [";", ",", "\t"]:
            df = pd.read_csv(StringIO(r.text), sep=sep, dtype=str, low_memory=False)
            if len(df.columns) > 3:
                logger.info(f"CSV parsé sep='{sep}' — {len(df)} lignes, {len(df.columns)} colonnes")
                return df
    raise ValueError("Aucun séparateur valide trouvé.")
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _extract_coords(df: pd.DataFrame) -> pd.DataFrame:
    """Extrait lat/lon depuis les colonnes 'coordinates_from' et 'coordinates_to'."""
    for col, lat_out, lon_out in [
        ("coordinates_from", "lat_from", "lon_from"),
        ("coordinates_to",   "lat_to",   "lon_to"),
    ]:
        if col in df.columns:
            parsed = df[col].apply(_parse_coords)
            df[lat_out] = parsed.apply(lambda x: x[0])
            df[lon_out] = parsed.apply(lambda x: x[1])
        else:
            df[lat_out] = None
            df[lon_out] = None
    return df


def _build_dim_gare(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col_name, col_lat, col_lon, col_ctry in [
        ("from", "lat_from", "lon_from", "from_nuts-0"),
        ("to",   "lat_to",   "lon_to",   "to_nuts-0"),
    ]:
        if col_name not in df.columns:
            logger.warning(f"Colonne '{col_name}' introuvable.")
            continue
        tmp = df[[col_name]].copy()
        tmp.columns = ["name"]
        tmp["latitude"] = df[col_lat].astype(float) if col_lat in df.columns else None
        tmp["longitude"] = df[col_lon].astype(float) if col_lon in df.columns else None
        tmp["city"] = None
        tmp["country"] = df[col_ctry] if col_ctry in df.columns else None
        tmp["is_main_station"] = False
        rows.append(tmp)

    if not rows:
        return pd.DataFrame()

    gares = pd.concat(rows, ignore_index=True).drop_duplicates(subset=["name"])
    gares = gares[gares["name"].notna() & (gares["name"].str.strip() != "")]
    gares["gare_id"] = range(1, len(gares) + 1)
    logger.info(f"DIM_GARE : {len(gares)} gares distinctes.")
    return gares[["gare_id", "name", "city", "country", "latitude", "longitude", "is_main_station"]]


def _build_dim_operateur(df: pd.DataFrame) -> pd.DataFrame:
    if "operator" not in df.columns:
        logger.warning("Colonne 'operator' introuvable.")
        return pd.DataFrame()
    ops = df[["operator"]].drop_duplicates().dropna().copy()
    ops.columns = ["agency_name"]
    ops = ops[ops["agency_name"].str.strip() != ""]
    ops["agency_id"] = ops["agency_name"].apply(lambda x: _stable_id(x, "OP_"))
    ops["agency_url"] = None
    ops["agency_country"] = None
    logger.info(f"DIM_OPERATEUR : {len(ops)} opérateurs distincts.")
    return ops[["agency_id", "agency_name", "agency_url", "agency_country"]]


def _build_dim_route(df: pd.DataFrame) -> pd.DataFrame:
    if "from" not in df.columns or "to" not in df.columns:
        logger.warning("Colonnes 'from'/'to' introuvables.")
        return pd.DataFrame()

    cols = ["from", "to"] + [c for c in ["operator", "from_nuts-0"] if c in df.columns]
    routes = df[cols].drop_duplicates(subset=["from", "to"]).dropna(subset=["from", "to"]).copy()
    routes = routes[(routes["from"].str.strip() != "") & (routes["to"].str.strip() != "")]
    routes.rename(columns={"from": "origin", "to": "destination"}, inplace=True)

    routes["route_id"] = range(1, len(routes) + 1)
    routes["route_long_name"] = routes["origin"] + " - " + routes["destination"]
    routes["agency_id"] = routes["operator"].apply(lambda x: _stable_id(x, "OP_") if pd.notna(x) else None) \
                                if "operator" in routes.columns else None
    routes["countries"] = routes["from_nuts-0"] if "from_nuts-0" in routes.columns else None

    logger.info(f"DIM_ROUTE : {len(routes)} routes distinctes.")
    return routes[["route_id", "agency_id", "route_long_name", "origin", "destination", "countries"]]


def _build_dim_train(df: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
    if "from" not in df.columns or "to" not in df.columns:
        return pd.DataFrame()

    cols = ["from", "to", "distance"] + (["operator"] if "operator" in df.columns else [])
    trains = df[cols].drop_duplicates().dropna(subset=["from", "to"]).copy()
    trains.rename(columns={"from": "origin", "to": "destination"}, inplace=True)

    if not routes.empty:
        trains = trains.merge(routes[["route_id", "origin", "destination"]], on=["origin", "destination"], how="left")
    else:
        trains["route_id"] = None

    trains["trip_id"] = [_stable_id(f"{r.origin}|{r.destination}|{i}", "TR_") for i, r in trains.iterrows()]
    trains["trip_headsign"] = trains["destination"]
    trains["duration"] = None
    trains["is_night_train"] = False

    logger.info(f"DIM_TRAIN : {len(trains)} trains distincts.")
    return trains[["trip_id", "route_id", "trip_headsign", "origin", "destination", "duration", "distance", "is_night_train"]]


def _build_dim_date() -> pd.DataFrame:
    logger.info("DIM_DATE : 1 entrée (snapshot été 2023).")
    return pd.DataFrame([{
        "date_id": 1, "start_date": SNAPSHOT_DATE, "end_date": SNAPSHOT_END,
        "monday": True, "tuesday": True, "wednesday": True, "thursday": True,
        "friday": True, "saturday": True, "sunday": True,
    }])


def _build_fact(df: pd.DataFrame, trains: pd.DataFrame, routes: pd.DataFrame,
                gares: pd.DataFrame, ops: pd.DataFrame) -> pd.DataFrame:
    if "from" not in df.columns or "to" not in df.columns:
        return pd.DataFrame()

    cols = ["from", "to"] + (["operator"] if "operator" in df.columns else [])
    fact = df[cols].dropna(subset=["from", "to"]).copy()
    fact.rename(columns={"from": "origin", "to": "destination"}, inplace=True)

    gare_map  = gares.set_index("name")["gare_id"].to_dict()  if not gares.empty  else {}
    op_map    = ops.set_index("agency_name")["agency_id"].to_dict() if not ops.empty else {}
    route_map = routes.set_index(["origin", "destination"])["route_id"].to_dict() if not routes.empty else {}
    train_map = trains.set_index(["origin", "destination"])["trip_id"].to_dict()  if not trains.empty else {}

    fact["gare_depart_id"] = fact["origin"].map(gare_map)
    fact["gare_arrivee_id"] = fact["destination"].map(gare_map)
    fact["route_id"] = fact.apply(lambda r: route_map.get((r["origin"], r["destination"])), axis=1)
    fact["train_id"] = fact.apply(lambda r: train_map.get((r["origin"], r["destination"])), axis=1)
    fact["operator_id"] = fact["operator"].map(op_map) if "operator" in fact.columns else None
    fact["date_id"] = 1

    # Distance Haversine
    # lat_map = gares.set_index("name")["latitude"].to_dict() if not gares.empty else {}
    # lon_map = gares.set_index("name")["longitude"].to_dict() if not gares.empty else {}
    # if any(v is not None for v in lat_map.values()):
    #     fact["distance_km"] = fact.apply(
    #         lambda r: _haversine_km(lat_map.get(r["origin"]), lon_map.get(r["origin"]),
    #                                 lat_map.get(r["destination"]), lon_map.get(r["destination"])), axis=1)
    # else:
    #     fact["distance_km"] = None
    fact["distance_km"] = trains["distance"]

    fact["duree_minutes"] = None

    CO2_PER_KM_TRAIN = 0.014  # kg per passenger-km
    fact["emissions_co2"] = (
        fact["distance_km"].astype(float)
        * CO2_PER_KM_TRAIN
    )
    
    fact["passengers"] = None
    fact["average_speed"] = None
    fact["is_night_train"] = False
    fact["fact_id"] = range(1, len(fact) + 1)

    logger.info(f"FACT_TRAJET_TRAIN : {len(fact)} lignes.")
    return fact[["fact_id", "train_id", "route_id", "operator_id",
                 "gare_depart_id", "gare_arrivee_id", "date_id",
                 "distance_km", "duree_minutes", "emissions_co2",
                 "passengers", "average_speed", "is_night_train"]]


def get_data_europa(local_csv: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    Transforme le dataset Europa intermodal rail.

    Args:
        local_csv: chemin vers CSV local (optionnel, sinon téléchargement auto).

    Retourne un dict {table_name: DataFrame} et exporte chaque table en CSV.
    """
    raw = _load_csv(CSV_URL, local_csv)
    raw = _normalize_columns(raw)
    raw = _extract_coords(raw)

    logger.info(f"Colonnes : {list(raw.columns)}")

    dim_gare = _build_dim_gare(raw)
    dim_operateur = _build_dim_operateur(raw)
    dim_route = _build_dim_route(raw)
    dim_train = _build_dim_train(raw, dim_route)
    dim_date = _build_dim_date()
    fact = _build_fact(raw, dim_train, dim_route, dim_gare, dim_operateur)

    result = {
        "dim_gare": dim_gare,
        "dim_operateur": dim_operateur,
        "dim_route": dim_route,
        "dim_train": dim_train,
        "dim_date": dim_date,
        "fact_trajet_train": fact,
    }

    # Export CSV
    for name, df in result.items():
        if not df.empty:
            path = f"{name}.csv"
            # df.to_csv(path, index=False)
            logger.info(f"  {name:25s} → {len(df):>6} lignes  [{path}]")
        else:
            logger.warning(f"  {name:25s} → vide.")

    return result

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
#     datasets = get_data_europa(local_csv="Intermodal_connections_Europe.csv")
#     for name, df in datasets.items():
#         print(f"\n=== {name} ({len(df)} lignes) ===")
#         print(df.head(3).to_string(index=False) if not df.empty else "  [vide]")