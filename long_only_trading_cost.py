import pandas as pd
from models import engine

def get_long_only_trading_cost():

    my_sql = """SELECT entry_date,(long_amer+long_emea) as long_usd FROM alto_daily order by entry_date;"""
    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = """SELECT trade_date,side,country_id,sum(notional_usd) as notional_usd FROM trade T1 JOIN product T2 on T1.product_id=T2.id 
    JOIN exchange T3 on T2.exchange_id=T3.id
    where position_side='Long' and parent_fund_id=1 and trade_date>='2019-04-01' and prod_type='Cash'
    group by trade_date,side,exchange_id;"""

    df_trade = pd.read_sql(my_sql, con=engine, parse_dates=['trade_date'])

    my_sql = """SELECT country_id,rate FROM anandaprod.exec_fee WHERE parent_broker_id=1 and rate_type='bp' and country_id not in (40,234);"""
    df_fee = pd.read_sql(my_sql, con=engine)

    # add rate=2 for country_id 40,234
    df_fee = df_fee._append({'country_id': 40, 'rate': 2}, ignore_index=True)
    df_fee = df_fee._append({'country_id': 234, 'rate': 2}, ignore_index=True)

    df_trade = pd.merge(df_trade, df_fee, how='left', on='country_id')
    df_trade['fee'] = abs(df_trade['notional_usd'])  * df_trade['rate'] / 10000

    # get df_trade pivot table
    df_trade_pivot = df_trade.pivot_table(index='trade_date', columns='side', values='fee', aggfunc='sum').reset_index()
    df_trade_pivot = df_trade_pivot.fillna(0)
    df_trade_pivot['total_fee'] = df_trade_pivot['B'] + df_trade_pivot['S']

    df = pd.merge(df_long, df_trade_pivot, how='left', left_on='entry_date', right_on='trade_date')
    df = df.fillna(0)

    df['fee_bp'] = df['total_fee'] / df['long_usd'] * 10000

    # group by year: keep fee_bp sum only
    df['year'] = df['entry_date'].dt.year
    df_group = df.groupby('year').agg({'fee_bp': 'sum'}).reset_index()

    pass



if __name__ == '__main__':
    get_long_only_trading_cost()