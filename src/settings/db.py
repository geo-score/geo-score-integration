from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, declarative_base

from settings.config import settings

engine = create_engine(settings.database_url, echo=False)
Base = declarative_base()


def get_session() -> Session:
    return Session(engine)


def ensure_postgis():
    """Make sure PostGIS extensions are enabled."""
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis_topology"))
        conn.commit()
