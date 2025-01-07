from datetime import date, timedelta
from models import ProductAction, engine, session
import pandas as pd
import numpy as np


def last_alpha_date():
    today = date.today()
    if today.weekday() == 0:
        end_date = today - timedelta(days=3)
    else:
        end_date = today - timedelta(days=1)
    with engine.connect() as con:
        rs = con.execute("SELECT max(entry_date) FROM position WHERE alpha_usd is not NULL;")
        for row in rs:
            max_date = row[0]
        end_date = min(max_date, end_date)
    return end_date


if __name__ == '__main__':

    last_date = last_alpha_date()
    today = date.today()
    previous_day = today - timedelta(days=1)

    my_sql = f"""SELECT T2.ticker,product_id,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
                     WHERE entry_date='{today}' and quantity>0 and prod_type='Cash' and parent_fund_id=1"""
    df_position = pd.read_sql(my_sql, con=engine, index_col='ticker')

    # get all product_id in a list
    prod_id_list_string = ','.join(map(str, df_position['product_id']))


    my_sql = f"""select entry_date,T2.ticker,price as real_px from product_market_data T1 join product T2 on T1.product_id=T2.id 
                where prod_type='Cash' and entry_date in ('2019-04-01', '2021-12-31','{previous_day}') and product_id in ({prod_id_list_string});"""
    df_real_px = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # convert into date
    df_real_px['entry_date'] = pd.to_datetime(df_real_px['entry_date']).dt.date

    # pivot
    df_real_px = df_real_px.pivot(index='ticker', columns='entry_date', values='real_px')

    stock_split_list = session.query(ProductAction).filter(ProductAction.action_type == 'Stock Split').all()
    for stock_split in stock_split_list:
        if stock_split.product.ticker in df_real_px.index.tolist():
            for col in df_real_px.columns:
                stock_split_date = stock_split.entry_date
                if stock_split_date > col:
                    df_real_px.loc[df_real_px.index == stock_split.product.ticker, col] = \
                        df_real_px.loc[df_real_px.index == stock_split.product.ticker, col] / stock_split.amount

    df_position = df_position.join(df_real_px, how='left')

    pass