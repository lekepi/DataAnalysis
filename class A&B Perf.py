import sys

import pandas as pd
from models import engine, session, Product, FundFee, NavAccountStatement
import numpy as np
from datetime import timedelta, date
import time


def get_class_perf(my_class='A', cncy='USD'):
    # get last fee for that class
    management_fee_rate = session.query(FundFee).filter_by(class_type=my_class, fee_type='Management').order_by(
        FundFee.entry_date.desc()).first().value
    perf_fee_rate = session.query(FundFee).filter_by(class_type=my_class, fee_type='Perf').order_by(
        FundFee.entry_date.desc()).first().value
    hurdle_fee_rate = session.query(FundFee).filter_by(class_type=my_class, fee_type='Hurdle').order_by(
        FundFee.entry_date.desc()).first().value

    if cncy == 'EUR':
        cncy_str = 'EURO'
    else:
        cncy_str = cncy

    if my_class == 'A':
        no_fee_class = 'F'
    else:
        no_fee_class = 'L'

    no_fee_full_class_name = f'{cncy_str} SHARES CLASS {no_fee_class}'

    my_sql = f"""SELECT entry_date,total_gross_ror FROM nav_ror WHERE class_name='{no_fee_full_class_name}' 
        and entry_date>='2019-04-01' order by entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df.index = df.index.date

    # get month to date (no calculation for current month if the month end pack not there)
    last_date = df.index[-1]
    next_month_date = last_date.replace(day=1) + timedelta(days=32)
    second_next_month_date = next_month_date.replace(day=1) + timedelta(days=32)
    max_date = second_next_month_date .replace(day=1) - timedelta(days=1)
    data_name = f'RETURN {cncy_str} CLASS {no_fee_class}'

    nav_statement = session.query(NavAccountStatement).filter_by(status='Daily', data_name=data_name).\
        filter(NavAccountStatement.entry_date <= max_date).filter(NavAccountStatement.entry_date >= next_month_date).\
        order_by(NavAccountStatement.entry_date.desc()).first()

    if nav_statement:
        net_ror = nav_statement.data_mtd
        statement_date = nav_statement.entry_date
        df.loc[statement_date, 'total_gross_ror'] = net_ror/100
    else:
        statement_date = date.today()

    # Perform vectorized calculations
    df['Begin Capital'] = np.nan
    df['Gross Profit'] = np.nan
    df['management fee'] = np.nan
    df['Incentive Fee'] = np.nan
    df['Ending Capital'] = np.nan
    df['Net ROR'] = np.nan
    df['management fee base'] = np.nan
    df['Hurdle Base'] = np.nan
    df['Hurdle Amount'] = np.nan
    df['Profit forward'] = np.nan
    df['Period Outperf'] = np.nan
    df['Cum Profit'] = np.nan
    df['Cum Fee'] = np.nan

    previous_index = None
    for index, row in df.iterrows():
        gross_ror = row['total_gross_ror']
        if not previous_index:
            begin_capital = 100
            hurdle_base = 0
            hurdle_amount = hurdle_fee_rate
            profit_forward = 0
        else:
            begin_capital = df.at[previous_index, 'Ending Capital']
            profit_forward = previous_row['Cum Profit']  # can be overridden is crystallization
            if index.month == 1:
                hurdle_base = begin_capital
                hurdle_amount = hurdle_base * hurdle_fee_rate / 100
                if previous_row['Cum Profit'] > 0:
                    profit_forward = 0
            else:
                hurdle_base = previous_row['Hurdle Base']
                hurdle_amount = 0
                profit_forward = previous_row['Cum Profit']

        gross_profit = gross_ror * begin_capital
        management_fee_base = begin_capital + gross_profit
        if index == statement_date:
            management_fee = 0
            # because we take the NAV NET ROR from the no incentive fee class (so management fees are included)
        else:
            management_fee = - management_fee_base * management_fee_rate / 100 / 12

        period_outperf = gross_profit + management_fee - hurdle_amount
        cum_profit = period_outperf + profit_forward
        cum_fee = np.maximum(cum_profit * perf_fee_rate / 100, 0)
        incentive_fee = -cum_fee
        if previous_index:
            if index.month != 1:
                incentive_fee += previous_row['Cum Fee']

        ending_capital = begin_capital + gross_profit + management_fee + incentive_fee
        net_ror = (gross_profit + management_fee + incentive_fee) / begin_capital

        df.at[index, 'Begin Capital'] = begin_capital
        df.at[index, 'Gross Profit'] = gross_profit
        df.at[index, 'management fee'] = management_fee
        df.at[index, 'Incentive Fee'] = incentive_fee
        df.at[index, 'Ending Capital'] = ending_capital
        df.at[index, 'Net ROR'] = net_ror
        df.at[index, 'management fee base'] = management_fee_base
        # df.at[index, 'management fee rate']
        df.at[index, 'Hurdle Base'] = hurdle_base
        df.at[index, 'Hurdle Amount'] = hurdle_amount
        df.at[index, 'Profit forward'] = profit_forward
        df.at[index, 'Period Outperf'] = period_outperf
        df.at[index, 'Cum Profit'] = cum_profit
        df.at[index, 'Cum Fee'] = cum_fee

        previous_index = index
        previous_row = df.loc[index]

    # store into excel
    df.to_excel(f'Excel\perf_{my_class}_{cncy}.xlsx')


if __name__ == '__main__':

    get_class_perf('B', 'USD')

    sys.exit()

    start_time = time.time()
    # get_class_perf('A', 'USD')
    get_class_perf('A', 'USD')
    get_class_perf('B', 'USD')
    get_class_perf('A', 'EUR')
    get_class_perf('B', 'EUR')
    get_class_perf('A', 'GBP')
    get_class_perf('B', 'GBP')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed time1:", elapsed_time, "seconds")


    print("Elapsed time2:", elapsed_time, "seconds")