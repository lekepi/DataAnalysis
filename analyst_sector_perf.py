from models import engine
import pandas as pd
from datetime import date


def get_analyst_sector_perf(sector_list, analyst, start_date):

    sector_list_str = "'" + "','".join(sector_list) + "'"

    # get all product_id that have an avg current size>0
    my_sql = f"""SELECT product_id,avg(current_size) as avg_size FROM analyst_perf WHERE is_historic=0 and is_top_pick=0 and 
    last_date>'2022-01-01' and last_date>'{start_date}' group by product_id HAVING avg(current_size)>0;"""
    df_long = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT T2.id as product_id,ticker FROM analyst_universe T1 JOIN PRODUCT T2 on T1.product_id=T2.id join user T3 on 
    T1.user_id=T3.id JOIN ananda_sector T4 on T1.ananda_sector_id=T4.id WHERE end_date is NULL and 
    T3.first_name='{analyst}' and T4.short_name in ({sector_list_str});"""
    df_ticker = pd.read_sql(my_sql, con=engine)

    df_ticker_filtered = pd.merge(df_long, df_ticker, on='product_id', how='inner')
    ticker_list = df_ticker_filtered['ticker'].tolist()

    today = date.today()
    # Long notional USD
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as long_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and T1.parent_fund_id=1 and entry_date>='{start_date}' and entry_date<'{today}' group by T1.entry_date
        Order by T1.entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    df['Total Size'] = 0
    df['Day Alpha'] = 0
    df['Analyst Alpha'] = 0
    for ticker in ticker_list:
        my_sql = f"""SELECT entry_date,alpha FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id 
            WHERE T2.ticker='{ticker}' and entry_date>='{start_date}' and entry_date<'{today}' order by entry_date;"""
        df_temp = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
        # rename alpha column
        df_temp.rename(columns={'alpha': f'day_alpha {ticker}'}, inplace=True)
        df = pd.concat([df, df_temp], axis=1)

        my_sql = f"""SELECT last_date as entry_date,alpha_point,current_size FROM analyst_perf T1 JOIN product T2 on 
            T1.product_id=T2.id WHERE is_historic=0 and is_top_pick=0 and ticker='{ticker}' and last_date>='{start_date}'
            order by entry_date;"""
        df_analyst = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
        # rename alpha column
        df_analyst.rename(columns={'alpha_point': f'analyst_alpha {ticker}'}, inplace=True)
        df_analyst.rename(columns={'current_size': f'current_size {ticker}'}, inplace=True)

        df = pd.concat([df, df_analyst], axis=1)
        # fill na
        df.fillna(0, inplace=True)
        df['Total Size'] += df[f'current_size {ticker}']
        df['Day Alpha'] += df[f'day_alpha {ticker}']
        df['Analyst Alpha'] += df[f'analyst_alpha {ticker}']

    df['Day Alpha'] = df['Day Alpha'] / len(ticker_list)
    avg_size = df['Total Size'].mean()
    df['avg_size'] = avg_size

    df['Static Alpha'] = df['avg_size'] * df['Day Alpha'] * 100
    df['Dynamic Alpha'] = df['Total Size'] * df['Day Alpha'] * 100

    df['Analyst Alpha cum'] = df['Analyst Alpha'].cumsum()
    df['Dynamic Alpha cum'] = df['Dynamic Alpha'].cumsum()
    df['Static Alpha cum'] = df['Static Alpha'].cumsum()
    df['Diff'] = df['Analyst Alpha cum'] - df['Static Alpha cum']

    # save to excel

    if len(sector_list) == 1:
        sector_str = sector_list[0]
        sector_str = sector_str.replace('/', '-')
    else:
        sector_str = 'Multi'
    df.to_excel(f'Excel\\Analyst Sector\\{analyst} {sector_str} Alpha Analysis.xlsx')


if __name__ == '__main__':

    sector_list = ['Aero', 'Automation', 'Autos', 'Consumer Low', 'Digital', 'Financials', 'Gaming', 'Healthcare',
                   'Industrial High', 'Industrial Low', 'Industrials', 'Ingredients/Salmon', 'Media', 'Packaging',
                   'Paneuro Consumer', 'Payments', 'Real assets', 'Roll ups', 'Semis', 'Tech Consumer', 'Tech Software',
                   'UK Consumer', 'US Consumer']

    analyst_list = ['Araceli', 'Alex', 'Samson']

    sector_list = ['Digital', 'Paneuro Consumer', 'Healthcare', 'Industrial Low', 'Ingredients/Salmon']  # 'Digital'
    analyst = 'Araceli'  # 'Araceli'

    start_date = date(2022, 1, 1)
    get_analyst_sector_perf(sector_list, analyst, start_date)
