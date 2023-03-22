import pandas as pd
from models import engine, session, Product
import numpy as np
from datetime import date, timedelta


def get_daily_df(analyst):
    my_sql = f"""SELECT last_date,alpha_point,current_size,is_historic,is_top_pick,ticker,T3.size,T3.ananda_sector_id,T4.name as ananda_sector
                 FROM analyst_perf T1 JOIN user T2 on T1.user_id=T2.id 
                 LEFT JOIN Product T3 on T1.product_id=T3.id LEFT JOIN ananda_sector T4 on T3.ananda_sector_id=T4.id 
                 WHERE T2.first_name='{analyst}' and last_date>'2022-01-01' order by last_date;"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['last_date'])

    df_current = df[(df['is_historic'] == 0) & (df['is_top_pick'] == 0)]
    # df is grouped by last_date and the sum of alpha_point and current_size is calculated and last_date is the index
    df_daily = df_current.groupby('last_date').agg({'alpha_point': 'sum', 'current_size': 'sum'}).reset_index()

    df_current_long = df_current[df_current['current_size'] >= 0]
    # df is grouped by last_date and the sum of alpha_point and current_size is calculated and last_date is the index
    df_current_long = df_current_long.groupby('last_date').agg({'alpha_point': 'sum', 'current_size': 'sum'}).reset_index()

    df_current_short = df_current[df_current['current_size'] < 0]
    # df is grouped by last_date and the sum of alpha_point and current_size is calculated and last_date is the index
    df_current_short = df_current_short.groupby('last_date').agg({'alpha_point': 'sum', 'current_size': 'sum'}).reset_index()

    # we merge df, df_long and df_short on last_date
    df_daily = pd.merge(df_daily, df_current_long, on='last_date', how='left', suffixes=('', '_long'))
    df_daily = pd.merge(df_daily, df_current_short, on='last_date', how='left', suffixes=('', '_short'))

    df_historic = df[(df['is_historic'] == 1) & (df['is_top_pick'] == 0)]
    # df is grouped by last_date and the sum of alpha_point and current_size is calculated and last_date is the index
    df_historic = df_historic.groupby('last_date').agg({'alpha_point': 'sum', 'current_size': 'sum'}).reset_index()
    # merge it to df_daily
    df_daily = pd.merge(df_daily, df_historic, on='last_date', how='left', suffixes=('', '_historic'))

    df_top_pick = df[(df['is_historic'] == 0) & (df['is_top_pick'] == 1)]
    # df is grouped by last_date and the sum of alpha_point and current_size is calculated and last_date is the index
    df_top_pick = df_top_pick.groupby('last_date').agg({'alpha_point': 'sum', 'current_size': 'sum'}).reset_index()

    # merge it to df_daily
    df_daily = pd.merge(df_daily, df_top_pick, on='last_date', how='left', suffixes=('', '_top_pick'))

    # group df_daily per quarter
    df_daily['year_quarter'] = df_daily['last_date'].dt.year.astype(str) + '-Q' + \
                               df_daily['last_date'].dt.quarter.astype(str)

    print(1)


if __name__ == '__main__':
    analyst_list = ['Araceli', 'Samson', 'Alex']
    for analyst in analyst_list:
        get_daily_df(analyst)
