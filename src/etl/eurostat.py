"""
eurostat_etl.py
---------------
ETL pipeline for fetching and transforming Eurostat TSV data.
Produces clean, typed DataFrames ready for database ingestion.
"""

import logging
import re
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

VALUE_PATTERN = re.compile(r"(\d+\.?\d*)")


def extract(url: str) -> pd.DataFrame:
    """Fetch a Eurostat TSV file and return the raw DataFrame."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return pd.read_csv(StringIO(response.text), sep="\t", dtype=str)
    except requests.RequestException as e:
        logger.error("Extract failed: %s", e)
        return pd.DataFrame()


def transform(df_raw: pd.DataFrame, drop_cols: list[str] | None, dataset_name: str = "") -> pd.DataFrame:
    """
    Parse a raw Eurostat TSV DataFrame into a clean long-format table.
    Adds a 'dataset' column for database provenance tracking.
    """
    if df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()
    df.columns = df.columns.str.strip()

    # Parse the composite first column, e.g. "freq,train,vehicle\TIME_PERIOD"
    composite_col = df.columns[0]
    if "\\" not in composite_col:
        logger.error("Unexpected first column format: '%s'", composite_col)
        return pd.DataFrame()

    dim_part, time_col = composite_col.split("\\", 1)
    dim_names = [d.strip() for d in dim_part.split(",")]
    time_col = time_col.strip() or "period"

    # Melt to long format
    df_long = df.melt(id_vars=[composite_col], var_name=time_col, value_name="value_raw")
    df_long[time_col] = df_long[time_col].str.strip()

    # Split composite column into individual dimension columns
    split_data = df_long[composite_col].str.split(",", expand=True)
    if split_data.shape[1] != len(dim_names):
        logger.error("Dimension mismatch: expected %d, got %d", len(dim_names), split_data.shape[1])
        return pd.DataFrame()

    split_data.columns = dim_names
    df_long = pd.concat([df_long.drop(columns=[composite_col]), split_data], axis=1)

    # Drop unwanted columns (e.g. 'freq')
    if drop_cols:
        df_long.drop(columns=[c for c in drop_cols if c in df_long.columns], inplace=True)
        dim_names = [d for d in dim_names if d not in drop_cols]

    # Extract numeric value, stripping Eurostat flags (e.g. '123p', ':', 'e')
    df_long["obs_value"] = pd.to_numeric(
        df_long["value_raw"].str.extract(VALUE_PATTERN)[0], errors="coerce"
    )
    df_long.drop(columns=["value_raw"], inplace=True)

    # Strip whitespace from all string columns
    str_cols = df_long.select_dtypes(include="object").columns
    df_long[str_cols] = df_long[str_cols].apply(lambda s: s.str.strip())

    # Add provenance and sort
    # df_long["dataset"] = dataset_name
    df_long.sort_values(by=dim_names + [time_col], inplace=True, ignore_index=True)

    logger.info("[%s] %d rows, columns: %s", dataset_name, len(df_long), df_long.columns.tolist())
    return df_long


def run_pipeline(url: str, dataset_name: str, drop_cols: list[str] | None) -> pd.DataFrame:
    """Run extract + transform for a single dataset."""
    logger.info("Processing '%s'...", dataset_name)
    return transform(extract(url), drop_cols=drop_cols, dataset_name=dataset_name)


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------
DATASETS = {
    "rail_tf_traveh":  "http://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/rail_tf_traveh/1.0?format=TSV&compress=false&c[TIME_PERIOD]=ge:2018",
    "rail_tf_passmov": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/rail_tf_passmov/1.0?format=TSV&compress=false&c[TIME_PERIOD]=ge:2018",
    "rail_pa_total":   "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/rail_pa_total/1.0?format=TSV&compress=false&c[TIME_PERIOD]=ge:2018",
}

if __name__ == "__main__":
    results = {
        name: run_pipeline(url, dataset_name=name, drop_cols=["freq"])
        for name, url in DATASETS.items()
    }

    for name, df in results.items():
        print(f"\n=== {name} ({len(df):,} rows) ===")
        print(df.head(10).to_string(index=False))

    # -- Database loading goes here --
    # from sqlalchemy import create_engine
    # engine = create_engine("postgresql://user:pass@host/db")
    # for name, df in results.items():
    #     if not df.empty:
    #         df.to_sql(name, engine, if_exists="replace", index=False)