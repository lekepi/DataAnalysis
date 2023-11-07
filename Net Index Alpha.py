import pandas as pd
from models import engine

if __name__ == '__main__':

    ticker = 'DGE LN'

    # sxxr
    my_sql = "SELECT entry_date,perf_1d as SXXR FROM index_return T1 JOIN product T2 on T1.product_id=T2.id WHERE ticker='SXXR Index' order by entry_date;"
    df_sxxr = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # sxxp
    my_sql = "SELECT entry_date,perf_1d as SXXP FROM index_return T1 JOIN product T2 on T1.product_id=T2.id WHERE ticker='SXXP Index' order by entry_date;"
    df_sxxp = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')


    my_sql = f"SELECT entry_date,ticker,adj_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id WHERE ticker in ('{ticker}','SXXR Index','SXXP Index') and entry_date>='2019-03-29';"
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    # pivot df on ticker
    df = df.pivot(columns='ticker', values='adj_price')
    # get 1d returns
    df = df.pct_change(1)
    # merge df with sxxr and sxxp
    df = pd.merge(df, df_sxxr, left_index=True, right_index=True)
    df = pd.merge(df, df_sxxp, left_index=True, right_index=True)

    my_sql = f"SELECT entry_date,ticker,beta,alpha FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id and ticker='{ticker}' order by entry_date;"
    df_beta = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_beta = df_beta.drop(columns=['ticker'])
    # merge
    df = pd.merge(df, df_beta, left_index=True, right_index=True)

    df['alpha_sxxp'] = df[ticker] - df['SXXP'] * df['beta']
    df['alpha_sxxr'] = df[ticker] - df['SXXR'] * df['beta']

    print(1)
