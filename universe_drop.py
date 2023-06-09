import pandas as pd
from models import engine


if __name__ == "__main__":
    my_sql = "SELECT entry_date,alpha_universe FROM alpha_summary WHERE parent_fund_id=1 order by entry_date asc;"
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df = df.sort_index(ascending=True)
    df['alpha_cum'] = df['alpha_universe'].cumsum()
    df['drop'] = 0

    # find last index
    last_index = df.index[-1]

    for index, row in df.iterrows():
        # get minimum in df['alpha_universe'] from index and index+4:
        start_index = max(index-4, 0)

        current = df.loc[index, 'alpha_cum']
        my_max = df.loc[start_index:index, 'alpha_cum'].max()
        my_drop = my_max - current

        # add column to df
        df.loc[index, 'drop'] = my_drop

    print(df)
