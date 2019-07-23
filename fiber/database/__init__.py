import sqlalchemy
import pandas as pd

from fiber import VERBOSE
from fiber.utils import tqdm, Timer

# DO NOT INCREASE THE CHUNK SIZE BEYOND THIS SIZE
# The Hana client will fail silently, returning only a subset of rows
READ_CHUNK_SIZE = 30_000


def _check_for_message_size_overflow(e):
    if 'Parameter row too large' in str(e.orig):
        raise RuntimeError(
            'Your statement had too many parameters to be handled '
            'by the database client.\n'
            'Increase the MAX_MESSAGE_SIZE in your PyHDB installation.'
        )


def read_with_progress(statement, engine):

    with Timer() as t:
        try:
            chunks = pd.read_sql(
                statement, con=engine, chunksize=READ_CHUNK_SIZE)
            result = pd.concat([
                x for x
                in tqdm(chunks, disable=(not VERBOSE))
            ])
        except sqlalchemy.exc.DataError as e:
            _check_for_message_size_overflow(e)

    print(f'Execution time: {t.elapsed:.2f}s')
    return result
