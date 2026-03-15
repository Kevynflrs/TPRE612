from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#Configuration
DB_CONFIG = {
    "dbname": "TPRE612",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": 5432,
}
SCHEMA = "tpre612_data_warehouse"

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

#SQLAlchemy
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"options": f"-csearch_path={SCHEMA}"},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
Base.metadata.schema = SCHEMA


def get_db():
    """Dépendance FastAPI : fournit une session DB et la ferme après usage."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
