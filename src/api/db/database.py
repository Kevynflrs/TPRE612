import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv(Path(__file__).resolve().parents[3] / ".env")

#Configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "TPRE612"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "1234"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
}
SCHEMA = os.getenv("API_DB_SCHEMA", "tpre612_data_warehouse")

DATABASE_URL = (
    f"postgresql://{quote_plus(str(DB_CONFIG['user']))}:{quote_plus(str(DB_CONFIG['password']))}"
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
