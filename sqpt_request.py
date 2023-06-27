# squarepoint requested to have the alto portfolio position for the first business day of each month
# the output follows their format


from datetime import date, timedelta
import pandas as pd
from models import engine, session, Aum


def get_start_month_weekday_list():

    start_date = date(2020, 4, 1)
    date_list = [start_date ]
    final_day = date(2023, 4, 3)

    while start_date < final_day:
        start_date = start_date.replace(day=1) + timedelta(days=32)
        start_date = start_date.replace(day=1)
        # if weekend go forward to the monday
        if start_date.weekday() == 5:
            start_date = start_date + timedelta(days=2)
        elif start_date.weekday() == 6:
            start_date = start_date + timedelta(days=1)

        if start_date == date(2021, 1, 1):
            start_date = date(2021, 1, 4)

        date_list.append(start_date)

    return date_list


def get_start_month_position():
    date_list = get_start_month_weekday_list()

    # I want to have a string with the list of date separated with commas
    date_list = ','.join([f"'{date}'" for date in date_list])

    my_sql = f"""SELECT entry_date as Date,T2.ticker as Symbol,'ticker_bbg' as Symbology,mkt_value_usd as Delta_usd FROM position T1
                JOIN product T2 on T1.product_id=T2.id
                WHERE entry_date in ({date_list}) and parent_fund_id=1 and prod_type in ('Cash','Future') 
                and T2.ticker not in ('AGI US', 'FNV US','FNV CN','NEM US','GOLD US','AEM US','GDX US','GC1 CMX','GLD US', 'ED1 CME', 'TY1 CBT')
                order by entry_date,mkt_value_usd desc;"""
    df = pd.read_sql(my_sql, con=engine)

    my_sql = """SELECT entry_date as Date,amount*1000000 as nav_usd FROM aum WHERE entry_date>='2020-03-01' and entry_date<'2023-05-01';"""
    df_aum = pd.read_sql(my_sql, con=engine)
    # merge on entry_date
    df = pd.merge(df, df_aum, how='left', left_on='Date', right_on='Date')

    # reformat Date to be dd/mm/yyyy
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%d/%m/%Y')

    # replace symbol 'SXO1 EUX' with 'SXO1 Index'
    df['Symbol'] = df['Symbol'].replace('SXO1 EUX', 'SXO1 Index')
    # replace symbol 'ES1 CME' with 'ES1 Index'
    df['Symbol'] = df['Symbol'].replace('ES1 CME', 'ES1 Index')

    print(1)


if __name__ == '__main__':
    get_start_month_position()
