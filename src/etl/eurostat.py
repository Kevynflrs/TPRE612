import re
from io import StringIO
from typing import List
import pandas as pd
import requests
import logging
from functools import reduce
import operator
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


DATASETS_CONFIG = {
    "rail_tf_traveh": {
        "url": "http://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/rail_tf_traveh/1.0?format=TSV&compress=false&c[TIME_PERIOD]=ge:2018",
        "drop_cols": ["freq"],
        "name": "train_traffic_source_energy"
    },
    "rail_tf_passmov": {
        "url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/rail_tf_passmov/1.0?format=TSV&compress=false&c[TIME_PERIOD]=ge:2018",
        "drop_cols": ["freq"],
        "name": "passenger_traffic_train_speed"
    },
    "rail_pa_total": {
        "url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/rail_pa_total/1.0?format=TSV&compress=false&c[TIME_PERIOD]=ge:2018",
        "drop_cols": ["freq"],
        "name": "passenger_transported"
    },
    "env_ac_ainah_r2": {
        "url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/env_ac_ainah_r2/1.0?format=TSV&compress=false&c[TIME_PERIOD]=ge:2018",
        "drop_cols": ["freq"],
        "keep_rows": [{"nace_r2": "H49"}, {"nace_r2": "H51"}],
        "name": "air_emissions"
    },
}

VALUE_PATTERN = re.compile(r"(\d+\.?\d*)")


def fetch_data(url: str) -> pd.DataFrame:
    """Télécharge le fichier TSV et le convertit en DataFrame brut."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        # On lit tout en string (dtype=str) pour éviter les erreurs de parsing initiales
        return pd.read_csv(StringIO(response.text), sep="\t", dtype=str)
    except Exception as e:
        logger.error(f"Erreur lors du téléchargement : {e}")
        return pd.DataFrame()


def transform_data(df: pd.DataFrame, drop_cols: List[str], keep_rows: List[dict] = None) -> pd.DataFrame:
    """
    Nettoie les données : transforme le format large en format long,
    sépare les colonnes combinées et supprime les colonnes inutiles.
    """
    if df.empty:
        return df

    # Nettoyage des noms de colonnes
    df.columns = df.columns.str.strip()

    # La première colonne est complexe (ex: "freq,train,vehicle\TIME_PERIOD")
    first_col = df.columns[0]
    if "\\" not in first_col:
        logger.error(f"Format de colonne inattendu : {first_col}")
        return pd.DataFrame()

    # Séparation "Dimensions" \ "Temps"
    dims_str, time_col_name = first_col.split("\\", 1)
    dim_names = [d.strip() for d in dims_str.split(",")]
    time_col_name = time_col_name.strip() or "period"

    # 1. Melt : Passage du format large (années en colonnes) au format long (lignes)
    df_long = df.melt(id_vars=[first_col],
                      var_name=time_col_name, value_name="raw_value")

    # 2. Split : Éclatement de la colonne composite en plusieurs colonnes
    # Ex: "A,B,C" devient Colonne1: A, Colonne2: B, Colonne3: C
    split_dims = df_long[first_col].str.split(",", expand=True)

    # Vérification de sécurité
    if split_dims.shape[1] != len(dim_names):
        logger.error("Erreur: Le nombre de dimensions ne correspond pas.")
        return pd.DataFrame()

    split_dims.columns = dim_names

    # On remplace la colonne composite par les nouvelles colonnes propres
    df_clean = pd.concat(
        [split_dims, df_long.drop(columns=[first_col])], axis=1)

    # 3. Nettoyage des valeurs numériques (extraction des chiffres, ignore les flags 'e', 'p')
    df_clean["obs_value"] = pd.to_numeric(
        df_clean["raw_value"].str.extract(VALUE_PATTERN)[0], errors="coerce"
    )
    df_clean.drop(columns=["raw_value"], inplace=True)

    # 4. Suppression des colonnes demandées (Feature demandée)
    if drop_cols:
        existing_cols_to_drop = [c for c in drop_cols if c in df_clean.columns]
        if existing_cols_to_drop:
            df_clean.drop(columns=existing_cols_to_drop, inplace=True)
            logger.info(f"Colonnes supprimées : {existing_cols_to_drop}")

    # 5. Nettoyage final (espaces vides)
    str_cols = df_clean.select_dtypes(include="object").columns
    df_clean[str_cols] = df_clean[str_cols].apply(lambda x: x.str.strip())
    
    # 6. Filtrage des lignes selon les critères spécifiques (si fournis)
    if keep_rows:
        logger.info(f"Lignes avant filtrage : {len(df_clean)}")
        logger.info(f"Colonnes disponibles : {df_clean.columns.tolist()}")
        
        # Check if filter columns exist
        for filter_dict in keep_rows:
            for col in filter_dict.keys():
                if col not in df_clean.columns:
                    logger.warning(f"⚠️  Colonne '{col}' introuvable dans le DataFrame!")
                else:
                    unique_vals = df_clean[col].unique()
                    logger.info(f"Valeurs uniques dans '{col}': {unique_vals[:10]}... (total: {len(unique_vals)})")

        # Vectorized: Create a mask for each filter dict (AND within, OR between)
        masks = [
            reduce(operator.and_,
                   (df_clean[col] == val for col, val in filter_dict.items()))
            for filter_dict in keep_rows
        ]
        
        # Log individual mask counts
        for i, mask in enumerate(masks):
            logger.info(f"Mask {i} ({keep_rows[i]}): {mask.sum()} lignes correspondent")
        
        # Combine all masks with OR
        final_mask = reduce(operator.or_, masks)
        logger.info(f"Masque final combiné (OR): {final_mask.sum()} lignes")
        
        df_clean = df_clean[final_mask]
        logger.info(f"Lignes après filtrage : {len(df_clean)}")

    return df_clean


def get_eurostat_data():
    eurostat_data = {}
    for id, config in DATASETS_CONFIG.items():
        logger.info(f"--- Traitement de : {id} ---")

        # A. Extract
        df_raw = fetch_data(config["url"])

        # B. Transform (avec les colonnes spécifiques à supprimer)
        df_clean = transform_data(
            df_raw, drop_cols=config["drop_cols"], keep_rows=config.get("keep_rows", None))

        eurostat_data[config["name"]] = df_clean

    return eurostat_data


get_eurostat_data()