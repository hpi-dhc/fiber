import os

from sqlalchemy import (
    create_engine,
    MetaData,
)

from fiber import (
    CACHE_PATH,
    DB_USER,
    DB_PASSWD,
    DB_HOST,
    DB_PORT,
    DB_SCHEMA
)
from fiber.database.meta import add_tables

DATABASE_URI = (
    f'mysql+pymysql://{DB_USER}:{DB_PASSWD}' +
    f'@{DB_HOST}:{DB_PORT}/{DB_SCHEMA}'
)

cache_file = os.path.join(CACHE_PATH, 'metadata.pkl')

engine = create_engine(DATABASE_URI)

meta = add_tables(MetaData(bind=engine))
