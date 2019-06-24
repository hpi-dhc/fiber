import pandas as pd
from tqdm import tqdm


def read_with_progress(statement, engine):

    chunks = pd.read_sql(statement, con=engine, chunksize=100000)

    df = pd.DataFrame()
    for chunk in tqdm(chunks):
        df = pd.concat([df, chunk])
    return df
