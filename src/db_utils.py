from sqlalchemy import create_engine
from src.config import DB_URL
import logging

def get_db_engine():
    """
    Create and return an SQLAlchemy Engine object.
    """
    try:
        engine = create_engine(DB_URL)
        # Test connect
        with engine.connect() as conn:
            pass
        return engine
    except Exception as e:
        logging.error(f"Cannot connect to Database at {DB_URL}. Error: {e}")
        raise e
