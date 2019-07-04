import os
import pickle
from contextlib import contextmanager
from getpass import getpass

import sqlparse
from sqlalchemy import (
    create_engine,
    MetaData,
)
from sqlalchemy.orm import sessionmaker

from fiber import CACHE_PATH, VERBOSE


PASSWD = os.getenv('FIBER_HANA_PASSWORD') or getpass('HANA Password: ')
# (TODO) use tunnel https://pypi.org/project/sshtunnel/
DATABASE_URI = f'hana://SYSTEM:{PASSWD}@la-forge.mssm.edu:39041/'

engine = create_engine(DATABASE_URI)
engine.execute('SET SCHEMA MSDW_2018;')

Session = sessionmaker(bind=engine)

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


def get_meta():
    try:
        with open(cache_file, 'rb') as f:
            meta = pickle.load(f)
    except FileNotFoundError:
        meta = MetaData(schema='MSDW_2018')
        meta.reflect(bind=engine)

        os.makedirs(CACHE_PATH)
        with open(cache_file, 'wb') as handle:
            pickle.dump(meta, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return meta


def compile_sqla(query_or_exp):
    if VERBOSE:
        compileable = getattr(query_or_exp, 'statement', query_or_exp)
        return sqlparse.format(str(
            compileable.compile(engine, compile_kwargs={"literal_binds": True})
        ), reindent=True)
    return "..."


def print_sqla(query_or_exp):
    print(compile_sqla(query_or_exp))
