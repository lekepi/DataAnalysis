from datetime import date, timedelta
from models import session, engine, AlphaUniverse
import pandas as pd
import math


def find_last_weekday(my_date):
    if my_date.weekday() == 5:
        my_date -= timedelta(days=1)
    elif my_date.weekday() == 6:
        my_date -= timedelta(days=2)
    return my_date


def get_universe_alpha(my_date):

    # delete the record if it exists
    session.query(AlphaUniverse).filter(AlphaUniverse.entry_date == my_date).delete()
    session.commit()

    for i in range(2):
        if i == 0:
            my_sql = f"""SELECT T2.ticker,alpha*10000 as alpha,T5.continent FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id
                         JOIN product_universe T3 on T3.product_id=T1.product_id JOIN exchange T4 on T4.id=T2.exchange_id
                         JOIN country T5 on T5.id=T4.country_id WHERE entry_date='{my_date}' and (end_date is NULL or end_date>='{my_date}') and start_date<='{my_date}';"""
        else:
            my_sql = f"""SELECT T2.ticker,alpha*10000 as alpha,T5.continent FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id
                         JOIN product_universe T3 on T3.product_id=T1.product_id JOIN exchange T4 on T4.id=T2.exchange_id
                         JOIN country T5 on T5.id=T4.country_id WHERE entry_date='{my_date}' and start_date='2019-04-01';"""

        df_universe = pd.read_sql(my_sql, con=engine, index_col='ticker')

        # get adj_price from product_market_data:
        previous_date = find_last_weekday(my_date - timedelta(days=1))
        my_sql = f"SELECT T2.ticker,adj_price as last_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id WHERE entry_date = '{previous_date}';"
        df_last = pd.read_sql(my_sql, con=engine, index_col='ticker')

        # find last weekday for the month before my_date
        month_date = find_last_weekday(date(my_date.year, my_date.month, 1) - timedelta(days=1))
        # get adj_price from product_market_data for month_date:
        my_sql = f"SELECT T2.ticker,adj_price as month_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id WHERE entry_date = '{month_date}';"
        df_month = pd.read_sql(my_sql, con=engine, index_col='ticker')

        if my_date.year == 2019:
            year_date = find_last_weekday(date(2019, 4, 1) - timedelta(days=1))
        else:
            year_date = find_last_weekday(date(my_date.year, 1, 1) - timedelta(days=1))
        # get adj_price from product_market_data for year_date:
        my_sql = f"SELECT T2.ticker,adj_price as year_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id WHERE entry_date = '{year_date}';"
        df_year = pd.read_sql(my_sql, con=engine, index_col='ticker')

        # join df_universe and df_last, df_month, df_year
        df = df_universe.join(df_last, how='left').join(df_month, how='left').join(df_year, how='left')

        df['perf_month'] = df['last_price'] / df['month_price']
        df['perf_year'] = df['last_price'] / df['year_price']

        temp_alpha_raw = df_universe['alpha'].mean()
        temp_alpha_amer = df_universe[df_universe['continent'] == 'AMER']['alpha'].mean()
        temp_alpha_emea = df_universe[df_universe['continent'] != 'AMER']['alpha'].mean()
        temp_alpha = temp_alpha_amer/3 + temp_alpha_emea*2/3

        temp_alpha_raw_m = (df['alpha'] * df['perf_month']).sum() / df['perf_month'].sum()
        temp_alpha_raw_y = (df['alpha'] * df['perf_year']).sum() / df['perf_year'].sum()

        df_amer = df[df['continent'] == 'AMER']
        df_emea = df[df['continent'] != 'AMER']

        temp_alpha_amer_m = (df_amer['alpha'] * df_amer['perf_month']).sum() / df_amer['perf_month'].sum()
        temp_alpha_amer_y = (df_amer['alpha'] * df_amer['perf_year']).sum() / df_amer['perf_year'].sum()

        temp_alpha_emea_m = (df_emea['alpha'] * df_emea['perf_month']).sum() / df_emea['perf_month'].sum()
        temp_alpha_emea_y = (df_emea['alpha'] * df_emea['perf_year']).sum() / df_emea['perf_year'].sum()

        temp_alpha_m = temp_alpha_amer_m/3 + temp_alpha_emea_m*2/3
        temp_alpha_y = temp_alpha_amer_y/3 + temp_alpha_emea_y*2/3

        if math.isnan(temp_alpha_raw_m):
            temp_alpha_raw_m = None
        if math.isnan(temp_alpha_raw_y):
            temp_alpha_raw_y = None
        if math.isnan(temp_alpha_amer_m):
            temp_alpha_amer_m = None
        if math.isnan(temp_alpha_amer_y):
            temp_alpha_amer_y = None
        if math.isnan(temp_alpha_emea_m):
            temp_alpha_emea_m = None
        if math.isnan(temp_alpha_emea_y):
            temp_alpha_emea_y = None
        if math.isnan(temp_alpha_m):
            temp_alpha_m = None
        if math.isnan(temp_alpha_y):
            temp_alpha_y = None


        if i == 0:
            alpha_raw = temp_alpha_raw
            alpha_amer = temp_alpha_amer
            alpha_emea = temp_alpha_emea
            alpha = temp_alpha
            alpha_raw_m = temp_alpha_raw_m
            alpha_raw_y = temp_alpha_raw_y
            alpha_amer_m = temp_alpha_amer_m
            alpha_amer_y = temp_alpha_amer_y
            alpha_emea_m = temp_alpha_emea_m
            alpha_emea_y = temp_alpha_emea_y
            alpha_m = temp_alpha_m
            alpha_y = temp_alpha_y
        else:
            alpha_raw_0 = temp_alpha_raw
            alpha_amer_0 = temp_alpha_amer
            alpha_emea_0 = temp_alpha_emea
            alpha_0 = temp_alpha
            alpha_raw_m_0 = temp_alpha_raw_m
            alpha_raw_y_0 = temp_alpha_raw_y
            alpha_amer_m_0 = temp_alpha_amer_m
            alpha_amer_y_0 = temp_alpha_amer_y
            alpha_emea_m_0 = temp_alpha_emea_m
            alpha_emea_y_0 = temp_alpha_emea_y
            alpha_m_0 = temp_alpha_m
            alpha_y_0 = temp_alpha_y

    new_universe = AlphaUniverse(entry_date=my_date,
                                 alpha_raw=alpha_raw,
                                 alpha_amer=alpha_amer,
                                 alpha_emea=alpha_emea,
                                 alpha=alpha,
                                 alpha_raw_0=alpha_raw_0,
                                 alpha_amer_0=alpha_amer_0,
                                 alpha_emea_0=alpha_emea_0,
                                 alpha_0=alpha_0,
                                 alpha_raw_m=alpha_raw_m,
                                 alpha_amer_m=alpha_amer_m,
                                 alpha_emea_m=alpha_emea_m,
                                 alpha_m=alpha_m,
                                 alpha_raw_m_0=alpha_raw_m_0,
                                 alpha_amer_m_0=alpha_amer_m_0,
                                 alpha_emea_m_0=alpha_emea_m_0,
                                 alpha_m_0=alpha_m_0,
                                 alpha_raw_y=alpha_raw_y,
                                 alpha_amer_y=alpha_amer_y,
                                 alpha_emea_y=alpha_emea_y,
                                 alpha_y=alpha_y,
                                 alpha_raw_y_0=alpha_raw_y_0,
                                 alpha_amer_y_0=alpha_amer_y_0,
                                 alpha_emea_y_0=alpha_emea_y_0,
                                 alpha_y_0=alpha_y_0)

    # add to db
    session.add(new_universe)
    session.commit()
    print(my_date)


if __name__ == '__main__':

    # my_mode = "Today"
    my_mode = "SpecificDay"
    my_mode = "RangeDays"

    if my_mode == "Today":
        my_date = date.today()
        get_universe_alpha(my_date)
    elif my_mode == "SpecificDay":
        my_date = date.today()
        day = timedelta(days=1)
        day3 = timedelta(days=3)
        if my_date.weekday() == 0:
            my_date -= day3
        else:
            my_date -= day
        my_date = date(2022, 4, 19)
        get_universe_alpha(my_date)
    else:  # Range / Loop
        my_date = date(2019, 4, 1)

        day = timedelta(days=1)
        while my_date < date.today():
            week_num = my_date.weekday()
            if week_num < 5:  # ignore Weekend
                get_universe_alpha(my_date)
            my_date += day
