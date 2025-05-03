from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from ..config import DATABASE_URL

Base = declarative_base()

def get_database_url():
    """Get database URL from config."""
    return DATABASE_URL

def init_db():
    """Initialize database connection and create tables."""
    engine = create_engine(get_database_url())
    Base.metadata.create_all(engine)
    return engine

def get_session():
    """Create a new database session."""
    engine = init_db()
    Session = sessionmaker(bind=engine)
    return Session() 