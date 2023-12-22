import pandas as pd
from models import engine

if __name__ =='__main__':
    my_sql = """SELECT product_id,avg(market_cap) as market_cap FROM product_market_cap WHERE entry_date>'2019-04-01' and type='Monthly' group by product_id;"""
    df = pd.read_sql(my_sql, con=engine)

    # if market_cap>3B, then large cap else small Cap
    df['mkt_cap_size'] = df['market_cap'].apply(lambda x: 'Large' if x > 3000 else 'Small')

    # get the list of id of large cap
    df_large = df.loc[df['mkt_cap_size'] == 'Large', 'product_id'].tolist()
    # create a string separeted by comma:
    str_large = ','.join(str(e) for e in df_large)

    # Long notional
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as large_notional_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and T1.parent_fund_id=1 and entry_date>='2019-04-01' and product_id in ({str_large}) group by T1.entry_date
        Order by T1.entry_date;"""

    df_long = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT T1.entry_date,sum(T1.alpha_usd) as large_alpha_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type in ('Cash','Future') 
        and T1.quantity<>0 and T1.parent_fund_id=1 and entry_date>='2019-04-01' and
        product_id in ({str_large}) group by T1.entry_date
        Order by T1.entry_date;"""
    df_large_alpha = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_large_alpha = df_large_alpha.join(df_long.set_index('entry_date'), how='left')

    df_large_alpha['large_alpha_perc'] = df_large_alpha['large_alpha_usd']/df_large_alpha['large_notional_usd']

    # get the list of id of small cap
    df_small = df.loc[df['mkt_cap_size']=='Small', 'product_id'].tolist()
    # create a string separeted by comma:
    str_small = ','.join(str(e) for e in df_small)

    # Long notional
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as small_notional_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and T1.parent_fund_id=1 and entry_date>='2019-04-01' and product_id in ({str_small}) and product_id<>4 group by T1.entry_date
        Order by T1.entry_date;"""

    df_long = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT T1.entry_date,sum(T1.alpha_usd) as small_alpha_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type in ('Cash','Future')
        and T1.quantity<>0 and T1.parent_fund_id=1 and entry_date>='2019-04-01' and
        product_id in ({str_small}) and product_id<>4 group by T1.entry_date
        Order by T1.entry_date;"""
    df_small_alpha = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_small_alpha = df_small_alpha.join(df_long.set_index('entry_date'), how='left')

    df_small_alpha['small_alpha_perc'] = df_small_alpha['small_alpha_usd']/df_small_alpha['small_notional_usd']

    # outer join the two dataframes
    df_alpha = df_large_alpha.join(df_small_alpha, how='outer')

    # remove last row
    df_alpha = df_alpha.iloc[:-1]

    # fill 'small_alpha_perc' and 'large_alpha_perc' with 0 when none
    df_alpha['small_alpha_perc'] = df_alpha['small_alpha_perc'].fillna(0)
    df_alpha['large_alpha_perc'] = df_alpha['large_alpha_perc'].fillna(0)

    df_alpha['small_alpha_perc_cum'] = df_alpha['small_alpha_perc'].cumsum()
    df_alpha['large_alpha_perc_cum'] = df_alpha['large_alpha_perc'].cumsum()


    print(1)