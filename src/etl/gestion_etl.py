import logging
import os
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# Use absolute imports instead of relative imports
import sys
sys.path.insert(0, str(Path(__file__).parent))

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from eurostat import get_eurostat_data
from night_train_data import get_night_train_data
from dataeuropa import get_data_europa
from data_gouv import get_data_gouv
from CO2 import get_co2_data
from sncf import get_sncf_data

# --- CONFIGURATION LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURATION BASE DE DONNÉES ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "TPRE612"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
}

ETL_SCHEMA = os.getenv("ETL_DB_SCHEMA", "tpre612_dataset_clean")


class DatabaseManager:
    """Classe utilitaire pour gérer la connexion et l'écriture en base de données."""
    
    def __init__(self, config: Dict, schema: str = "public"):
        url = (
            f"postgresql://{quote_plus(str(config['user']))}:{quote_plus(str(config['password']))}"
            f"@{config['host']}:{config['port']}/{config['dbname']}"
        )
        self.engine = create_engine(url)
        self.schema = schema

    def _upsert_method(self, table, conn, keys, data_iter):
        """
        Méthode "magique" pour faire un UPSERT (Insert ou Update) avec Pandas et PostgreSQL.
        """
        from sqlalchemy.dialects.postgresql import insert
        
        data = [dict(zip(keys, row)) for row in data_iter]
        if not data:
            return
            
        sql_table = table.table
        
        # On récupère les clés primaires pour savoir sur quoi vérifier le conflit
        primary_keys = [key.name for key in sql_table.primary_key]
        
        if not primary_keys:
            # Pas de clé primaire ? On fait un insert normal
            conn.execute(sql_table.insert(), data)
            return

        # On utilise insert() de SQLAlchemy avec clause ON CONFLICT
        stmt = insert(sql_table).values(data)
        
        # Si conflit sur la clé primaire, on met à jour toutes les autres colonnes
        # Build update dictionary: {col_name: excluded.col_name}
        update_dict = {
            col.name: stmt.excluded[col.name] 
            for col in sql_table.columns 
            if col.name not in primary_keys
        }
        
        if update_dict:
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=primary_keys, 
                set_=update_dict
            )
        else:
            # Si que des clés primaires, on ignore les doublons
            upsert_stmt = stmt.on_conflict_do_nothing(index_elements=primary_keys)

        conn.execute(upsert_stmt)

    def load_dataset(self, df: pd.DataFrame, table_name: str):
        """Charge le DataFrame en base. Crée la table ou fait un upsert si elle existe."""
        if df.empty:
            logger.warning(f"Dataset vide pour {table_name}, on saute.")
            return

        try:
            inspector = inspect(self.engine)
            
            # Si la table n'existe pas, on la crée proprement avec des clés primaires
            if not inspector.has_table(table_name, schema=self.schema):
                logger.info(f"Création de la table '{table_name}' dans le schéma '{self.schema}'...")
                # Toutes les colonnes sauf la valeur deviennent la clé primaire composite
                pk_cols = [c for c in df.columns if c != "obs_value"]
                
                # Écriture initiale (échoue si existe déjà, mais on a vérifié avant)
                df.to_sql(table_name, self.engine, if_exists='fail', index=False, schema=self.schema)
                
                # Ajout de la contrainte Primary Key (indispensable pour l'upsert futur)
                with self.engine.connect() as conn:
                    pk_str = ", ".join([f'"{c}"' for c in pk_cols])
                    conn.execute(text(f'ALTER TABLE "{self.schema}"."{table_name}" ADD PRIMARY KEY ({pk_str})'))
                    conn.commit()
            else:
                # Si la table existe, on utilise le mode UPSERT
                logger.info(f"Mise à jour (Upsert) de la table '{table_name}' ({len(df)} lignes)...")
                df.to_sql(
                    table_name, 
                    self.engine, 
                    if_exists='append', 
                    index=False, 
                    method=self._upsert_method,  # Utilise notre méthode personnalisée
                    schema=self.schema
                )
            
            logger.info(f"Succès pour '{table_name}'.")

        except Exception as e:
            logger.error(f"Erreur Base de Données sur '{table_name}': {e}")


def main():
    # Initialiser la connexion DB
    try:
        db = DatabaseManager(DB_CONFIG, schema=ETL_SCHEMA)
        logger.info("Connexion Base de données OK.")
    except Exception as e:
        logger.critical(f"Impossible de se connecter à la DB : {e}")
        return

    # Eurostat Data
    euostat_dat = get_eurostat_data()
    for name, df in euostat_dat.items():
        if not df.empty:
            print(f"Aperçu Eurostat {name}:\n", df.head(3).to_string(index=False))
        db.load_dataset(df, table_name=name)
    

    # Night Train Data
    night_train_data = get_night_train_data()
    for name, df in night_train_data.items():
        if not df.empty:
            print(f"Aperçu Night Train Data {name}:\n", df.head(3).to_string(index=False))
        db.load_dataset(df, table_name=name)

    # Data.europa.eu
    europa_data = get_data_europa()
    for name, df in europa_data.items():
        if not df.empty:
            print(f"Aperçu Data.europa.eu {name}:\n", df.head(3).to_string(index=False))
        db.load_dataset(df, table_name=name)

    # Data.gouv.fr
    gouv_data = get_data_gouv()
    for name, df in gouv_data.items():
        if not df.empty:
            print(f"Aperçu Data.gouv.fr {name}:\n", df.head(3).to_string(index=False))
        db.load_dataset(df, table_name=name)
        
    # CO2 Data
    co2_data = get_co2_data()
    if co2_data is not None:
        db.load_dataset(co2_data, table_name="co2_emissions")
    else:
        logger.warning("Dataset vide pour co2_emissions, on saute.")

    # SNCF Data
    sncf_data = get_sncf_data()
    db.load_dataset(sncf_data, table_name="sncf_emissions")

if __name__ == "__main__":
    main()