import os
from getpass import getpass

__author__ = 'Tom Martensen, Philipp Bode, Christoph Anders, Jonas Kopka'
__version__ = '0.1.1'

CACHE_PATH = os.path.join(os.path.dirname(__file__), '.cache')
DEFAULT_STORE_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    '.store/default.yaml'
)
VERBOSE = (
    os.getenv('FIBER_VERBOSE') in (
        'true',
        'True',
        '1',
        'yes',
    )
    or False
)

DB_TYPE = os.getenv('FIBER_DB_TYPE') or 'hana'
if not DB_TYPE == 'test':
    DB_USER = os.getenv('FIBER_DB_USER') or input('DB User: ')
    DB_PASSWD = os.getenv('FIBER_DB_PASSWORD') or getpass('DB Password: ')
    DB_HOST = os.getenv('FIBER_DB_HOST') or input('DB Host: ')
    DB_PORT = os.getenv('FIBER_DB_PORT') or input('DB Port: ')
    DB_SCHEMA = os.getenv('FIBER_DB_SCHEMA') or input('DB Schema: ')

OCCURRENCE_INDEX = ['medical_record_number', 'age_in_days']
