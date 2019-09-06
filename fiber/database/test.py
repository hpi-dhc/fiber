import os

from sqlalchemy import (
   create_engine,
   MetaData,
)
from sqlalchemy.orm import sessionmaker

from fiber.database.meta import add_tables

database_path = os.path.join(
   os.path.dirname(__file__),
   './../../tests/mock_data.db'
)
DATABASE_URI = f'sqlite:///{database_path}'

engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

meta = add_tables(MetaData())

meta.create_all(engine)
