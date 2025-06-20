from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from ..config import DATABASE_URL

import os

Base = declarative_base()

def get_database_url():
    """Get database URL from config."""
    if os.getenv('TEST_ENV'):
        return DATABASE_URL.replace('fis_data', 'fis_data_test')
    else:
        return DATABASE_URL

def init_db():
    """Initialize database connection."""
    return create_engine(get_database_url())

def get_session():
    """Create a new database session."""
    engine = init_db()
    Session = sessionmaker(bind=engine)
    return Session() 