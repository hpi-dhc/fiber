import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

from fiber.utils import Timer, tqdm
from fiber.database.hana import DATABASE_URI

engines = {
    'pyhdb': create_engine(DATABASE_URI),
    'hdbcli': create_engine(DATABASE_URI.replace('+pyhdb', '')),
}

CHUNKSIZE = 30_000


def _fetch_random(rows, engine, columns=1):
    return [x for x in pd.read_sql(
        f'''
            SELECT {','.join(['RAND()' for x in range(columns)])}
            FROM SERIES_GENERATE_INTEGER(1,1,{rows+1});
        ''',
        engine,
        chunksize=CHUNKSIZE
    )]


def benchmark_fetch():
    results = []

    try:
        for e in tqdm(range(2, 6), 'rows'):
            rows = 10 ** e
            for client, engine in tqdm(engines.items(), 'engine'):
                for _ in tqdm(range(3), 'try'):
                    with Timer() as t:
                        res = _fetch_random(rows, engine)
                    assert sum((len(x) for x in res)) == rows
                    results.append({
                        'rows': rows,
                        'time': t.elapsed,
                        'engine': client,
                    })
    except KeyboardInterrupt:
        pass

    df = pd.DataFrame(results)
    return df


def draw(df):
    ax = sns.lineplot(x="rows", y="time", hue='engine', data=df)
    ax.set_xticks(df.rows.unique())
    ax.set_xscale('log')
    ax.set_title(f'Single Column pd.read_sql (chunksize={CHUNKSIZE})')
    ax.set_ylim(0)
    return ax


if __name__ == '__main__':
    results = benchmark_fetch()
    draw(results)
    plt.savefig('benchmark.png')
