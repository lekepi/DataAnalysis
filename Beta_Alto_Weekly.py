import pandas as pd
from models import session, engine


if __name__ == '__main__':

    my_sql = f"""SELECT T1.entry_date,T1.pnl_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id
            LEFT JOIN exchange T3 on T2.exchange_id=T3.id LEFT JOIN country T4 on T3.country_id=T4.id
            LEFT JOIN industry_group_gics T5 on T2.industry_group_gics_id=T5.id WHERE parent_fund_id=1
            and entry_date>='2019-04-01' and (T2.prod_type='Cash' or 
            T2.ticker in ('ES1 CME', 'SXO1 EUX')) order by entry_date"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df['pnl_usd'] = df['pnl_usd'].fillna(0)
    df['month_year'] = df['entry_date'].dt.strftime('%Y-%m')
    df['week'] = df['entry_date'].dt.to_period('W').apply(lambda r: r.start_time)

    my_sql = f"""Select entry_date,amount*1000000 as denominator from aum where entry_date>='2019-04-01' 
                    and entry_date>='2019-04-01' and type='leveraged' and fund_id=4
                    order by entry_date"""
    df_denominator = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_denominator['month_year'] = df_denominator['entry_date'].dt.strftime('%Y-%m')
    # remove entry_date
    df_denominator = df_denominator.drop(columns=['entry_date'])
    df = df.merge(df_denominator, on='month_year', how='left')
    df['Alto Return'] = 2 * df['pnl_usd'] / df['denominator']

    df_group = df.groupby(['week']).agg({
        'Alto Return': 'sum',
    }).reset_index()
    df_group['week'] = df_group['week'].dt.date

    df_group['start_date'] = df_group['week'] - pd.to_timedelta(3, unit='d')

    my_sql = """SELECT entry_date,ticker,adj_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE ticker in ('SXXR Index','SPTR500N Index');"""
    df_price = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # pivot
    df_price = df_price.pivot(index='entry_date', columns='ticker', values='adj_price').reset_index()
    # convert index from datetime to date
    df_price['entry_date'] = df_price['entry_date'].dt.date

    # get ticker price for each week
    df_group = df_group.merge(df_price, left_on='start_date', right_on='entry_date', how='left')

    # add return for each index
    df_group['SXXR Return'] = df_group['SXXR Index'].pct_change()
    df_group['SPTR500N Return'] = df_group['SPTR500N Index'].pct_change()

    # move up one row
    df_group['SXXR Return'] = df_group['SXXR Return'].shift(-1)
    df_group['SPTR500N Return'] = df_group['SPTR500N Return'].shift(-1)

    # remove last row
    df_group = df_group[:-1]

    df_group['BM'] = df_group['SPTR500N Return']/3 + df_group['SXXR Return']*2/3

    print(1)
