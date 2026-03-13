import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from database import DatabaseManager, DB_CONFIG
import pandas as pd
from dim_date import populate_dim_date
from dim_energie import populate_dim_energie
from dim_operateur import populate_dim_operateur    
from dim_route import populate_dim_route
from dim_train import populate_dim_train
from dim_gare import populate_dim_gare
from fact_trajet_train import populate_fact_trajet_train, populate_all_from_clean


def main():
    try:
        db_clean = DatabaseManager(DB_CONFIG, schema="tpre612_dataset_clean")
        db_warehouse = DatabaseManager(
            DB_CONFIG, schema="tpre612_data_warehouse")
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return

    populate_dim_operateur(db_clean, db_warehouse)
    populate_dim_route(db_clean, db_warehouse)
    populate_dim_train(db_clean, db_warehouse)
    populate_dim_date(db_clean, db_warehouse)
    populate_dim_energie(db_clean, db_warehouse)
    populate_dim_gare(db_clean, db_warehouse)
    populate_fact_trajet_train(db_clean, db_warehouse)
    populate_all_from_clean(db_clean, db_warehouse)

if __name__ == "__main__":
    main()