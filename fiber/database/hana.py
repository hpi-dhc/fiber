import os
from contextlib import contextmanager
from getpass import getpass

from sqlalchemy import (
    create_engine,
    MetaData,
)
from sqlalchemy.orm import sessionmaker

from fiber import CACHE_PATH
from fiber.database.meta import add_tables


PASSWD = os.getenv('FIBER_HANA_PASSWORD') or getpass('HANA Password: ')
# (TODO) use tunnel https://pypi.org/project/sshtunnel/
DATABASE_URI = ***REMOVED***

engine = create_engine(DATABASE_URI)

Session = sessionmaker(bind=engine)
session = Session()

cache_file = os.path.join(CACHE_PATH, 'metadata.pkl')


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


meta = add_tables(MetaData(bind=engine, schema='MSDW_2018'))
