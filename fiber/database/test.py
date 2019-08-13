from sqlalchemy import (
   create_engine,
   MetaData,
)
from sqlalchemy.orm import sessionmaker

from fiber.database.meta import add_tables

DATABASE_URI = f'sqlite:///:memory:'

engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

meta = add_tables(MetaData())

meta.create_all(engine)
