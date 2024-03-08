import pandas as pd
from models import engine


def get_earning_alpha():
    my_sql = """SELECT T2.ticker,product_id,entry_date,alpha_usd from position T1 JOIN product T2 on T1.product_id=T2.id
    WHERE parent_fund_id=1 and entry_date>'2019-04-01' and alpha_usd<>0"""
    df_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = """SELECT product_id,entry_date from product_calendar where my_type='Earnings'
    order by product_id,entry_date"""
    df_earning = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df = pd.merge(df_pos, df_earning, on=['product_id', 'entry_date'], how='inner')

    my_sql = """SELECT trade_date as entry_date,product_id,sum(pnl_close) as trade_usd FROM trade WHERE parent_fund_id=1
                and pnl_close is not NULL GROUP by entry_date,product_id;"""
    df_trade = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df = pd.merge(df, df_trade, on=['product_id', 'entry_date'], how='left')

    df.to_excel("Excel\df_earning_alpha_by_name.xlsx", index=False)

    df_alpha = df.groupby('entry_date')[['alpha_usd', 'trade_usd']].sum().reset_index()
    df_alpha = df_alpha.sort_values(by='entry_date')
    df_alpha['alpha+trade_usd'] = df_alpha['alpha_usd'] + df_alpha['trade_usd']

    my_sql = "SELECT entry_date,long_usd FROM alpha_summary WHERE parent_fund_id=1 order by entry_date;"
    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_alpha = pd.merge(df_alpha, df_long, on=['entry_date'], how='left')

    df_alpha['Alpha %'] = df_alpha['alpha_usd'] / df_alpha['long_usd']
    df_alpha['Alpha+Trade %'] = df_alpha['alpha+trade_usd'] / df_alpha['long_usd']

    df_alpha['Cum. Alpha %'] = df_alpha['Alpha %'].cumsum()
    df_alpha['Cum. Alpha+Trade %'] = df_alpha['Alpha+Trade %'].cumsum()

    df_alpha.to_excel("Excel\df_earning_alpha.xlsx", index=False)


if __name__ == '__main__':
    get_earning_alpha()
