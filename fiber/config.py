import os
from getpass import getpass

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
    ) or False
)

MIN_DAYS_FILTER_ACTIVE = (
    os.getenv('FIBER_MIN_DAYS_FILTER_ACTIVE') in (
        'true',
        'True',
        '1',
        'yes'
    ) or False
)

DB_TYPE = os.getenv('FIBER_DB_TYPE') or input('DB Type (hana, mysql, test): ')
if not DB_TYPE == 'test':
    DB_USER = os.getenv('FIBER_DB_USER') or input('DB User: ')
    DB_PASSWD = os.getenv('FIBER_DB_PASSWORD') or getpass('DB Password: ')
    DB_HOST = os.getenv('FIBER_DB_HOST') or input('DB Host: ')
    DB_PORT = os.getenv('FIBER_DB_PORT') or input('DB Port: ')
    DB_SCHEMA = os.getenv('FIBER_DB_SCHEMA') or input('DB Schema: ')

else:
    database_path = os.getenv('FIBER_TEST_DB_PATH') or os.path.join(
        os.path.dirname(__file__),
        './../tests/mock_data.db'
    )
    DATABASE_URI = f'sqlite:///{database_path}'

OCCURRENCE_INDEX = ['medical_record_number', 'age_in_days']
