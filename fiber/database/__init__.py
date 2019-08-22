import sys
from importlib import import_module

import sqlparse
import pandas as pd
import pyhdb
from pyhdb.protocol.constants.general import MAX_MESSAGE_SIZE


import fiber
from fiber.utils import tqdm, Timer


def get_engine():
    return getattr(import_module(f'fiber.database.{fiber.MSDW_DB}'), 'engine')


def get_meta():
    return getattr(import_module(f'fiber.database.{fiber.MSDW_DB}'), 'meta')


# DO NOT INCREASE THE CHUNK SIZE BEYOND THIS SIZE
# The Hana client will fail silently, returning only a subset of rows
READ_CHUNK_SIZE = 30_000

MESSAGE_OVERFLOW_ERROR = RuntimeError(
    'Your statement was too large to be handled by the hana client.\n'
    'Increase the MAX_MESSAGE_SIZE in your PyHDB installation.'
)


def compile_sqla(query_or_clause, engine):
    compileable = getattr(query_or_clause, 'statement', query_or_clause)
    compiled = str(compileable.compile(
        engine, compile_kwargs={"literal_binds": True}))

    return compiled


def print_sqla(query_or_clause, engine):
    print(sqlparse.format(
        compile_sqla(query_or_clause, engine),
        reindent=True
    ))


def read_with_progress(query_or_statement, engine, silent=False):
    """
    TEST
    """

    if not isinstance(query_or_statement, str):
        # Explicitly compile to check for message size overflow
        # and avoid parameter limit of prepared statements.
        query_or_statement = compile_sqla(query_or_statement, engine)

    if (
        engine.dialect.dbapi is pyhdb
        and sys.getsizeof(query_or_statement) > MAX_MESSAGE_SIZE
    ):
        raise MESSAGE_OVERFLOW_ERROR

    if fiber.VERBOSE and not silent:
        print(sqlparse.format(query_or_statement, reindent=True))

    with Timer('Execution'):
        chunks = pd.read_sql_query(
            query_or_statement, con=engine, chunksize=READ_CHUNK_SIZE)
    with Timer('Fetching'):
        result = pd.concat([
            x for x
            in tqdm(chunks)
        ])

    return result
