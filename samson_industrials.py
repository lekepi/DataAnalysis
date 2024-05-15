from datetime import date
import pandas as pd
from models import engine


def get_alpha_analysis(ticker_list, start_date):

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

    df['Static Alpha'] = df['avg_size'] * df['Day Alpha']*100
    df['Dynamic Alpha'] = df['Total Size'] * df['Day Alpha']*100

    df['Analyst Alpha cum'] = df['Analyst Alpha'].cumsum()
    df['Dynamic Alpha cum'] = df['Dynamic Alpha'].cumsum()
    df['Static Alpha cum'] = df['Static Alpha'].cumsum()

    # save to excel
    df.to_excel('Excel\\Samson Industrial Alpha Analysis2.xlsx')



if __name__ == '__main__':
    ticker_list = ['SAND SS', 'WEIR LN', 'EPIA SS']
    start_date = date(2022, 1, 1)
    get_alpha_analysis(ticker_list, start_date)
