import pandas as pd
from models import engine
from datetime import date

def get_current_portfolio_perf():

    end_date = date(2023, 6, 16)
    start_date = date(2023, 5, 31)

    my_sql = f"""SELECT T2.ticker,T2.prod_type,T1.mkt_value_usd as end_usd FROM position T1 JOIN
                     product T2 on T1.product_id=T2.id WHERE entry_date='{end_date}' and parent_fund_id=1 and prod_type not in ('Call', 'Put', 'Roll'); """
    df_init = pd.read_sql(my_sql, con=engine)

    my_sql = f"SELECT T2.ticker,T1.adj_price as end_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id WHERE entry_date='{end_date}';"
    df_end_price = pd.read_sql(my_sql, con=engine)

    my_sql = f"SELECT T2.ticker,T1.adj_price as start_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id WHERE entry_date='{start_date}';"
    df_start_price = pd.read_sql(my_sql, con=engine)

    df = pd.merge(df_init, df_start_price)
    df = pd.merge(df, df_end_price)
    df['start_usd'] = df['end_usd'] / df['end_price'] * df['start_price']

    print(1)


if __name__ == '__main__':
    get_current_portfolio_perf()
