from sqlalchemy import (
   create_engine,
   MetaData,
)
from sqlalchemy.orm import sessionmaker

from fiber.config import DATABASE_URI
from fiber.database.meta import add_tables

engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

meta = add_tables(MetaData())

meta.create_all(engine)
