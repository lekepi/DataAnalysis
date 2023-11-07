from datetime import date, timedelta
from models import AlphaSummary, engine, session, PositionBacktest
import pandas as pd
from utils import find_past_date


def is_first_weekday_of_month(my_date):
    # Check if the day is a weekday (0 = Monday, 6 = Sunday)
    if my_date.day == 1 or (my_date.day == 2 and my_date.weekday() == 0) or (my_date.day == 3 and my_date.weekday() == 0):
        return True
    else:
        return False


def get_position_backtest_kennel(my_date):

    # delete all position backtest for the date
    session.query(PositionBacktest).filter(PositionBacktest.entry_date == my_date).filter(PositionBacktest.type == 'Kennel').delete()
    session.commit()

    df_beta = pd.read_sql(f"SELECT product_id,alpha as alpha_1d,return_1d FROM product_beta WHERE entry_date='{my_date}'", con=engine)
    position_backtest_list = []

    # if first day of the month rebalancing
    if is_first_weekday_of_month(my_date):
        alpha_summary = session.query(AlphaSummary).filter(AlphaSummary.entry_date == my_date).\
            filter(AlphaSummary.parent_fund_id == 1).first()
        long_amer_usd = alpha_summary.long_amer_usd
        long_emea_usd = alpha_summary.long_emea_usd

        # get all universe stocks
        my_sql = f"""SELECT T2.id as product_id,T2.ticker,continent FROM anandaprod.analyst_selection T1
                     JOIN product T2 on T1.product_id=T2.id
                     JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
                     WHERE (end_date is NULL or end_date>='{my_date}') and is_dog=1;"""
        df_universe = pd.read_sql(my_sql, con=engine)
        # merge with beta
        df_universe = pd.merge(df_universe, df_beta, on='product_id', how='left')

        # remove stocks with no beta
        df_universe = df_universe[df_universe['return_1d'].notna()]

        # count amer is the number of stocks where continent = 'AMER'
        count_amer = df_universe[df_universe['continent'] == 'AMER'].shape[0]
        # count emea is the number of stocks where continent = 'EMEA'
        count_emea = df_universe[df_universe['continent'] != 'AMER'].shape[0]

        amount_amer = long_amer_usd / count_amer
        amount_emea = long_emea_usd / count_emea

        for index, row in df_universe.iterrows():
            entry_date = my_date
            product_id = row['product_id']
            continent = row['continent']
            alpha_1d = row['alpha_1d']
            return_1d = row['return_1d']
            if continent == 'AMER':
                notional_usd = amount_amer
            else:
                notional_usd = amount_emea
            alpha_usd = alpha_1d * notional_usd
            pnl_usd = return_1d * notional_usd

            new_position_backtest = PositionBacktest(entry_date=entry_date,
                                                     product_id=product_id,
                                                     notional_usd=notional_usd,
                                                     alpha_usd=alpha_usd,
                                                     pnl_usd=pnl_usd,
                                                     alpha_1d=alpha_1d,
                                                     return_1d=return_1d,
                                                     type='Kennel')
            position_backtest_list.append(new_position_backtest)
    # else take previous record
    else:
        previous_day = find_past_date(my_date, 1)
        my_sql = f"SELECT entry_date,product_id,notional_usd,return_1d as return_1d_old FROM position_backtest WHERE entry_date='{previous_day}' and type='Kennel';"
        df_previous = pd.read_sql(my_sql, con=engine)
        df_universe = pd.merge(df_previous, df_beta, on='product_id', how='left')
        # remove stocks with no beta
        df_universe = df_universe[df_universe['return_1d'].notna()]

        for index, row in df_universe.iterrows():
            entry_date = my_date
            product_id = row['product_id']
            notional_usd = row['notional_usd'] * (1 + row['return_1d_old'])
            alpha_1d = row['alpha_1d']
            return_1d = row['return_1d']
            alpha_usd = alpha_1d * notional_usd
            pnl_usd = return_1d * notional_usd

            new_position_backtest = PositionBacktest(entry_date=entry_date,
                                                     product_id=product_id,
                                                     notional_usd=notional_usd,
                                                     alpha_usd=alpha_usd,
                                                     pnl_usd=pnl_usd,
                                                     alpha_1d=alpha_1d,
                                                     return_1d=return_1d,
                                                     type='Kennel')
            position_backtest_list.append(new_position_backtest)
    session.add_all(position_backtest_list)
    session.commit()
    print(my_date)


def get_position_backtest_universe(my_date):

    # delete all position backtest for the date
    session.query(PositionBacktest).filter(PositionBacktest.entry_date == my_date).filter(PositionBacktest.type == 'Universe').delete()
    session.commit()

    df_beta = pd.read_sql(f"SELECT product_id,alpha as alpha_1d,return_1d FROM product_beta WHERE entry_date='{my_date}'", con=engine)
    position_backtest_list = []

    # if first day of the month rebalancing
    if is_first_weekday_of_month(my_date):
        alpha_summary = session.query(AlphaSummary).filter(AlphaSummary.entry_date == my_date).\
            filter(AlphaSummary.parent_fund_id == 1).first()
        long_amer_usd = alpha_summary.long_amer_usd
        long_emea_usd = alpha_summary.long_emea_usd

        # get all universe stocks
        my_sql = f"""SELECT T2.id as product_id,T2.ticker,continent FROM product_universe T1 JOIN product T2 on T1.product_id=T2.id
                     JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id and 
                     start_date<='{my_date}' and (end_date is NULL or end_date>='{my_date}');"""
        df_universe = pd.read_sql(my_sql, con=engine)
        # merge with beta
        df_universe = pd.merge(df_universe, df_beta, on='product_id', how='left')

        # remove stocks with no return
        df_universe = df_universe[df_universe['return_1d'].notna()]

        # count amer is the number of stocks where continent = 'AMER'
        count_amer = df_universe[df_universe['continent'] == 'AMER'].shape[0]
        # count emea is the number of stocks where continent = 'EMEA'
        count_emea = df_universe[df_universe['continent'] != 'AMER'].shape[0]

        amount_amer = long_amer_usd / count_amer
        amount_emea = long_emea_usd / count_emea

        for index, row in df_universe.iterrows():
            entry_date = my_date
            product_id = row['product_id']
            continent = row['continent']
            alpha_1d = row['alpha_1d']
            return_1d = row['return_1d']
            if continent == 'AMER':
                notional_usd = amount_amer
            else:
                notional_usd = amount_emea
            alpha_usd = alpha_1d * notional_usd
            pnl_usd = return_1d * notional_usd

            new_position_backtest = PositionBacktest(entry_date=entry_date,
                                                     product_id=product_id,
                                                     notional_usd=notional_usd,
                                                     alpha_usd=alpha_usd,
                                                     pnl_usd=pnl_usd,
                                                     alpha_1d=alpha_1d,
                                                     return_1d=return_1d,
                                                     type='Universe')
            position_backtest_list.append(new_position_backtest)
    # else take previous record
    else:
        previous_day = find_past_date(my_date, 1)
        my_sql = f"SELECT entry_date,product_id,notional_usd,return_1d as return_1d_old FROM position_backtest WHERE entry_date='{previous_day}' and type='Universe';"
        df_previous = pd.read_sql(my_sql, con=engine)
        df_universe = pd.merge(df_previous, df_beta, on='product_id', how='left')
        # remove stocks with no beta
        df_universe = df_universe[df_universe['return_1d'].notna()]

        for index, row in df_universe.iterrows():
            entry_date = my_date
            product_id = row['product_id']
            notional_usd = row['notional_usd'] * (1 + row['return_1d_old'])
            alpha_1d = row['alpha_1d']
            return_1d = row['return_1d']
            alpha_usd = alpha_1d * notional_usd
            pnl_usd = return_1d * notional_usd

            new_position_backtest = PositionBacktest(entry_date=entry_date,
                                                     product_id=product_id,
                                                     notional_usd=notional_usd,
                                                     alpha_usd=alpha_usd,
                                                     pnl_usd=pnl_usd,
                                                     alpha_1d=alpha_1d,
                                                     return_1d=return_1d,
                                                     type='Universe')
            position_backtest_list.append(new_position_backtest)
    session.add_all(position_backtest_list)
    session.commit()
    print(my_date)


if __name__ == '__main__':

    # my_mode = "Today"
    # my_mode = "SpecificDay"
    my_mode = "RangeDays"

    if my_mode == "Today":
        my_date = date.today()
        day = timedelta(days=1)
        day3 = timedelta(days=3)
        if my_date.weekday() == 0:
            my_date -= day3
        else:
            my_date -= day
        get_position_backtest_kennel(my_date)
    elif my_mode == "SpecificDay":
        my_date = date(2019, 4, 1)
        # get_intraday_trade(my_date)
        get_position_backtest_kennel(my_date)
    else:  # Range / Loop
        my_date = date(2019, 4, 1)
        last_date = date.today()
        day = timedelta(days=1)
        while my_date < last_date:
            week_num = my_date.weekday()
            if week_num < 5:  # ignore Weekend
                get_position_backtest_kennel(my_date)
            my_date += day
        # save df into excel


