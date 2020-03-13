from contextlib import contextmanager

from sqlalchemy import (
    create_engine,
    MetaData,
)
from sqlalchemy.orm import sessionmaker

from fiber.config import (
    DB_HOST,
    DB_PASSWD,
    DB_PORT,
    DB_SCHEMA,
    DB_USER
)
from fiber.database.meta import add_tables

# (TODO) use tunnel https://pypi.org/project/sshtunnel/
DATABASE_URI = f'hana+pyhdb://{DB_USER}:{DB_PASSWD}@{DB_HOST}:{DB_PORT}/'

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)
session = Session()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        # session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


meta = add_tables(MetaData(bind=engine, schema=DB_SCHEMA))
