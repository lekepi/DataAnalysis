import pandas as pd
from models import engine
from datetime import date


def get_one_year_alpha():
    start_date = date(2019, 4, 1)
    end_date= date.today()

    my_sql = f"""SELECT T2.ticker,T1.entry_date,T1.entry_date as my_date,quantity,T1.alpha_usd FROM position T1 join product T2 on T1.product_id=T2.id
                WHERE entry_date>='{start_date}' and entry_date<='{end_date}' and T2.prod_type = 'Cash' and T1.parent_fund_id=1 and T1.quantity<>0 
                order by T2.ticker,T1.entry_date;"""
    df_alto = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date', 'my_date'], index_col='entry_date')

    # fill alpha_usd with 0
    df_alto['alpha_usd'] = df_alto['alpha_usd'].fillna(0)

    # Long notional USD
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as not_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and T1.parent_fund_id=1 and entry_date>='{start_date}' and entry_date<='{end_date}'
        group by T1.entry_date Order by T1.entry_date;"""
    df_notional = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    df_alto = df_alto.join(df_notional)
    df_alto['alpha_bp'] = df_alto['alpha_usd'] / df_alto['not_usd'] * 10000
    df_alto = df_alto.sort_values(['ticker', 'my_date'])

    df_alto = df_alto.drop(columns=['alpha_usd'])
    df_alto = df_alto.drop(columns=['not_usd'])
    df_alto = df_alto.drop(columns=['quantity'])

    df_result = pd.DataFrame()
    # get unique ticker from df_alto
    tickers = df_alto['ticker'].unique()
    # tickers = ['GRF SM']

    for ticker in tickers:
        df_ticker = df_alto.loc[df_alto['ticker'] == ticker]
        df_ticker = df_notional.join(df_ticker)
        df_ticker['ticker'] = ticker
        df_ticker['alpha_bp'] = df_ticker['alpha_bp'].fillna(0)
        df_ticker['alpha_bp_cum'] = df_ticker['alpha_bp'].cumsum()
        df_ticker['max_alpha_bp_cum'] = df_ticker['alpha_bp_cum'].rolling(window=520, min_periods=1).max()
        df_ticker['draw_down'] = df_ticker['alpha_bp_cum'] - df_ticker['max_alpha_bp_cum']

        df_ticker = df_ticker[df_ticker['draw_down'] < -200]

        if df_ticker is not None:
            df_result = pd.concat([df_result, df_ticker])

    df_result = df_result.drop(columns=['not_usd'])
    # save excel
    df_result.to_excel('excel\one_year_alpha.xlsx', index=True)


if __name__ == '__main__':
    get_one_year_alpha()
