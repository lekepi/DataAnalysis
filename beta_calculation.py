from datetime import date, timedelta
import pandas as pd
from models import engine


def get_beta(my_date):
    my_sql = f"""SELECT product_id,mkt_value_usd FROM anandaprod.position WHERE entry_date='{my_date}' and parent_fund_id=1;"""
    df = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT product_id,beta FROM product_beta WHERE entry_date ='{my_date}';"""
    df_beta = pd.read_sql(my_sql, con=engine)

    df = pd.merge(df, df_beta, on='product_id', how='left')
    df['beta'] = df['beta'].fillna(1)

    df['Notional_beta'] = df['mkt_value_usd'] * df['beta']
    beta_portfolio = df['Notional_beta'].sum() / df['mkt_value_usd'].sum()
    return beta_portfolio


if __name__ == '__main__':
    # my_mode = "Today"
    # my_mode = "SpecificDay"
    my_mode = "RangeDays"

    df_result = pd.DataFrame(columns=['Beta'])

    if my_mode == "Today":
        my_date = date.today()
        get_beta(my_date)
    elif my_mode == "SpecificDay":
        my_date = date.today()
        day = timedelta(days=1)
        day3 = timedelta(days=3)
        if my_date.weekday() == 0:
            my_date -= day3
        else:
            my_date -= day
        my_date = date(2022, 4, 19)
        get_beta(my_date)
    else:  # Range / Loop
        my_date = date(2019, 4, 1)

        day = timedelta(days=1)
        while my_date <= date.today():
            week_num = my_date.weekday()
            if week_num < 5:  # ignore Weekend
                beta = get_beta(my_date)
                df_result.loc[my_date] = beta
            my_date += day
        # export to Excel
        df_result.to_excel('Excel/beta_portfolio.xlsx')