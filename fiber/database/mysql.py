import os
from getpass import getpass

from sqlalchemy import create_engine


USER = os.getenv('FIBER_DB_USER') or input('DB User: ')
PASSWD = os.getenv('FIBER_DB_PASSWORD') or getpass('DB Password: ')
DATABASE_URI = f'mysql+pymysql://{USER}:{PASSWD}@la-forge.mssm.edu/msdw_2018'

engine = create_engine(DATABASE_URI)
