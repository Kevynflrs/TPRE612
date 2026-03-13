from sqlalchemy import Table, Column, MetaData, create_engine, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from typing import List, Dict
DB_CONFIG = {
    "dbname": "TPRE612",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": 5432
}


class DatabaseManager:
    def __init__(self, config: Dict, schema: str = "public"):
        url = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
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
