import pandas as pd
from models import engine
from datetime import date


def get_pair_mix_long_only(start_date, end_date, ticker1, ticker2, position_million):
    # Long notional USD
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as long_usd FROM position T1
    JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
    and T1.parent_fund_id=1 and entry_date>='{start_date}' and entry_date<='{end_date}' group by T1.entry_date
    Order by T1.entry_date;"""
    df_long_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as notional1,sum(T1.alpha_usd) as alpha1,
     sum(pnl_usd) as pnl1 FROM position T1
     JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' 
     and T1.quantity<>0 and T1.parent_fund_id=1 and entry_date>='{start_date}' and entry_date<='{end_date}' 
     and T2.Ticker='{ticker1}' group by T1.entry_date
     Order by T1.entry_date;"""
    df_stock1 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df = pd.concat([df_long_usd, df_stock1], axis=1)

    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as notional2,sum(T1.alpha_usd) as alpha2,
     sum(pnl_usd) as pnl2 FROM position T1
     JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' 
     and T1.quantity<>0 and T1.parent_fund_id=1 and entry_date>='{start_date}' and entry_date<='{end_date}' 
     and T2.Ticker='{ticker2}' group by T1.entry_date
     Order by T1.entry_date;"""
    df_stock2 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df = pd.concat([df, df_stock2], axis=1)
    df = df.fillna(0)

    df['alpha'] = df['alpha1'] + df['alpha2']
    df['notional'] = df['notional1'] + df['notional2']
    df['alto_size'] = df['notional'] / df['long_usd']
    df['size1'] = df['notional1'] / df['notional']
    df['size2'] = df['notional2'] / df['notional']

    my_sql = f"""SELECT entry_date,beta as day_beta1,alpha as day_alpha1,return_1d as day_return1
     FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id WHERE T2.ticker='{ticker1}' order by entry_date;"""
    df_day1 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df = pd.concat([df, df_day1], axis=1)

    my_sql = f"""SELECT entry_date,beta as day_beta2,alpha as day_alpha2,return_1d as day_return2
        FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id WHERE T2.ticker='{ticker2}' order by entry_date;"""
    df_day2 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df = pd.concat([df, df_day2], axis=1)

    my_sql = f"""SELECT T1.entry_date,adj_price as price1,price,volume as volume,rate FROM anandaprod.product_market_data T1 JOIN product T2 on T1.product_id=T2.id
JOIN currency_history T3 on T2.currency_id=T3.currency_id and T1.entry_date=T3.entry_date WHERE ticker='{ticker1}'
 and T1.entry_date>='{start_date}' and T1.entry_date<='{end_date}' ;"""
    df_price1 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_price1['volume1'] = df_price1['volume'] * df_price1['price'] / df_price1['rate']
    df_price1 = df_price1[['price1', 'volume1']]

    my_sql = f"""SELECT T1.entry_date,adj_price as price2,price,volume as volume,rate FROM anandaprod.product_market_data T1 JOIN product T2 on T1.product_id=T2.id
    JOIN currency_history T3 on T2.currency_id=T3.currency_id and T1.entry_date=T3.entry_date WHERE ticker='{ticker2}'
     and T1.entry_date>='{start_date}' and T1.entry_date<='{end_date}' ;"""
    df_price2 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_price2['volume2'] = df_price2['volume'] * df_price2['price'] / df_price2['rate']
    df_price2 = df_price2[['price2', 'volume2']]

    df = pd.concat([df, df_price1, df_price2], axis=1)

    df['alpha_alto %'] = df['alpha'] / df['long_usd']
    df['alpha_dyn_size_static_split %'] = (df['day_alpha1']+df['day_alpha2']) / 2 * df['alto_size']

    avg_size = df['alto_size'].mean()
    df['avg_size'] = avg_size
    df['alpha_static_size_static_split %'] = (df['day_alpha1']+df['day_alpha2']) / 2 * avg_size
    df['alpha_static_size_dyn_split %'] = (df['day_alpha1'] * df['size1'] + df['day_alpha2'] * df['size2']) * avg_size

    position_million = position_million * 1000000

    df['current_stock'] = None
    df['long_size1'] = None
    df['long_size2'] = None

    first_size1 = df['size1'].iloc[0]
    if first_size1 > 0.5:
        current_stock = ticker1
        size1 = 1
        size2 = 0
    else:
        current_stock = ticker2
        size1 = 0
        size2 = 1

    for index, row in df.iterrows():

        vol_10_perc = min(row['volume1'], row['volume2']) * 0.1 / position_million
        if current_stock == ticker1:
            size1 = min(size1 + vol_10_perc, 1)
            size2 = 1 - size1
        else:
            size2 = min(size2 + vol_10_perc, 1)
            size1 = 1 - size2

        df.loc[index, 'current_stock'] = current_stock
        df.loc[index, 'long_size1'] = size1
        df.loc[index, 'long_size2'] = size2

        size_1 = row['size1']
        size_2 = row['size2']
        if current_stock == ticker1 and size_1 == 0:
            current_stock = ticker2
        elif current_stock == ticker2 and size_2 == 0:
            current_stock = ticker1

    df['alpha_long_only %'] = (df['day_alpha1'] * df['long_size1'] + df['day_alpha2'] * df['long_size2']) * df['avg_size']

    df['alto %'] = df['alpha_alto %'].cumsum()
    df['dyn_size_static_split %'] = df['alpha_dyn_size_static_split %'].cumsum()
    df['static_size_static_split %'] = df['alpha_static_size_static_split %'].cumsum()
    df['static_size_dyn_split %'] = df['alpha_static_size_dyn_split %'].cumsum()
    df['long_only %'] = df['alpha_long_only %'].cumsum()

    pass


if __name__ == '__main__':
    start_date = date(2019, 4, 1)
    end_date = date.today()
    position_million = 50
    get_pair_mix_long_only(start_date, end_date, 'LR FP', 'ASSAB SS', position_million)
