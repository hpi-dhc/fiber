import os

__author__ = 'HPI FIBER Team'

CACHE_PATH = os.path.join(os.path.dirname(__file__), '.cache')
DEFAULT_STORE_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    '.store/default.yaml'
)
VERBOSE = os.environ.get('VERBOSE')
