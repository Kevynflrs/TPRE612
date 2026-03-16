from pathlib import Path
from typing import Dict, List
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert
import os


load_dotenv(Path(__file__).resolve().parents[3] / ".env")

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "TPRE612"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
}


class DatabaseManager:
    def __init__(self, config: Dict, schema: str = "public"):
        url = (
            f"postgresql://{quote_plus(str(config['user']))}:{quote_plus(str(config['password']))}"
            f"@{config['host']}:{config['port']}/{config['dbname']}"
        )
        self.engine = create_engine(url)
        self.schema = schema

    def get_data_from_table(self, table_name: str) -> pd.DataFrame:
        query = text(f"SELECT * FROM {self.schema}.{table_name}")
        with self.engine.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        return df


    def upsert(self, df: pd.DataFrame, table_name: str, conflict_columns: List[str], schema = None, batch_size: int = 1000) -> None:
        if df.empty:
            return
        schema = schema or self.schema
        columns = list(df.columns)
        table = Table(table_name, MetaData(), *
                    [Column(c) for c in columns], schema=schema)

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            records = batch.to_dict(orient="records")
            stmt = insert(table).values(records)

            update_cols = {c: stmt.excluded[c]
                        for c in columns if c not in conflict_columns}

            if update_cols:
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_columns, set_=update_cols)
            else:
                # All columns are conflict keys — nothing to update, just skip duplicates
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)

            with self.engine.begin() as conn:
                conn.execute(stmt)

        print(
            f"Upsert terminé : {len(df)} lignes en {((len(df) - 1) // batch_size) + 1} batch(es)")
