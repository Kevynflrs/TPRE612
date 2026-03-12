from sqlalchemy import create_engine, text
from sqlalchemy.engine.reflection import Inspector

# Database connection URL
db_url = "postgresql://postgres:1234@localhost:5432/TPRE612"
schema_name = "tpre612_dataset_clean"
# Replace with your actual credentials and database name

def get_first_three_rows(db_url):
    # Create SQLAlchemy engine
    engine = create_engine(db_url)

    # Get inspector
    inspector = Inspector.from_engine(engine)

    # Get all table names in the target schema
    table_names = inspector.get_table_names(schema=schema_name)

    for table_name in table_names:
        print(f"\n--- First 3 rows of table: {table_name} ---")

        # Get column names
        columns = inspector.get_columns(table_name, schema=schema_name)
        col_names = [col["name"] for col in columns]

        # Print column headers
        print("\t".join(col_names))

        # Execute query to fetch first 3 rows
        with engine.connect() as conn:
            query = text(f"SELECT * FROM {schema_name}.{table_name} LIMIT 3")
            result = conn.execute(query)
            for row in result:
                print("\t".join(str(field) for field in row))

if __name__ == "__main__":
    try:
        get_first_three_rows(db_url)
    except Exception as e:
        print(f"An error occurred: {e}")
