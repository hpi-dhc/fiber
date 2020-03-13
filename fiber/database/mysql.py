from sqlalchemy import (
    create_engine,
    MetaData,
)

from fiber.config import (
    DB_HOST,
    DB_PASSWD,
    DB_PORT,
    DB_SCHEMA,
    DB_USER
)
from fiber.database.meta import add_tables

DATABASE_URI = (
    f'mysql+pymysql://{DB_USER}:{DB_PASSWD}' +
    f'@{DB_HOST}:{DB_PORT}/{DB_SCHEMA}'
)

engine = create_engine(DATABASE_URI)

meta = add_tables(MetaData(bind=engine))
