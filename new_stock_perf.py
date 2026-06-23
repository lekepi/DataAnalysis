import pandas as pd
from models import engine
from datetime import date, timedelta


if __name__ == '__main__':

    date_2y = date.today() - timedelta(days=365 * 2)
    my_sql = f"""SELECT product_id,T2.ticker,min(entry_date) as after_date,prod_type FROM position T1 
    JOIN product T2 on T1.product_id=T2.id WHERE parent_fund_id=1
    and entry_date>'{date_2y}' group by T1.product_id,T2.ticker,prod_type;"""

    df_new = pd.read_sql(my_sql, con=engine, parse_dates=['after_date'])

    my_sql = f"""SELECT product_id,T2.ticker,max(entry_date) as before_date,prod_type FROM position T1 JOIN product T2 on
     T1.product_id=T2.id WHERE parent_fund_id=1 and entry_date<'{date_2y}'
     group by T1.product_id,T2.ticker,prod_type;"""
    df_old = pd.read_sql(my_sql, con=engine, parse_dates=['before_date'])

    # merge
    df = pd.merge(df_new, df_old, on=['product_id', 'ticker', 'prod_type'], how='left')
    df['date_diff'] = (df['after_date'] - df['before_date']).dt.days

    # keep only when date_diff is nan or > 730
    df = df[(df['date_diff'].isna()) | (df['date_diff'] > 730)]
    # filter prod_type is cash
    df = df[df['prod_type'] == 'Cash']
    # remove column
    df = df[['product_id', 'ticker', 'after_date', 'before_date']]

    # get all product_id in a list
    product_id_list = df['product_id'].tolist()
    product_id_str = ','.join([str(pid) for pid in product_id_list])

    my_sql = f"""SELECT entry_date,product_id,T2.ticker,alpha_usd FROM position T1 
    JOIN product T2 on T1.product_id=T2.id WHERE parent_fund_id=1 and prod_type='cash' and entry_date>'{date_2y}'
    and product_id in ({product_id_str});"""

    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = f"""SELECT entry_date,long_amer+long_emea as long_usd FROM alto_daily WHERE entry_date>'{date_2y}';"""
    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # merge df_position and df_long on entry_date
    df_position = pd.merge(df_position, df_long, on='entry_date', how='left')
    # calculate alpha_usd / long_usd
    df_position['alpha_bp'] = df_position['alpha_usd'] / df_position['long_usd'] * 10000

    # group by ticker, sum alpha_perf
    df_perf = df_position.groupby('ticker')['alpha_bp'].sum().reset_index()
    df = pd.merge(df, df_perf, on='ticker', how='left')

    # rename after_date: new_trade_date, rename before_date: old_trade_date
    df.rename(columns={'after_date': 'new_trade_date', 'before_date': 'old_trade_date'}, inplace=True)

    # sort by alpha_bp descending
    df.sort_values(by='alpha_bp', ascending=False, inplace=True)
    # remove product_id
    df = df[['ticker', 'new_trade_date', 'old_trade_date', 'alpha_bp']]
    # save to excel in excel folder
    df.to_excel('excel/new_stock_perf.xlsx', index=False)

    pass