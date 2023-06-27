# Calculate the PNL of all the trades from the specified date
# until today (similar function than the PNL USD column in EMSX)

from datetime import date, timedelta
from models import engine, TradePnl, session
import pandas as pd
import sys

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


def get_full_pnl(start_date, end_date):

    my_sql = f"""SELECT T1.id,trade_date,T2.ticker,T1.product_id,T1.notional_usd,pnl_close,parent_fund_id FROM trade T1
                JOIN product T2 on T1.product_id=T2.id WHERE trade_date>='{start_date}' and trade_date<'{end_date}'
                 and prod_type in ('Cash', ' Future') order by trade_date;"""
    #and parent_fund_id=1


    df_trade = pd.read_sql(my_sql, con=engine, parse_dates=['trade_date'], index_col='id')
    df_trade['Px1'] = None
    df_trade['Px2'] = None
    df_trade['Perf'] = None
    # get all distinct trade dates
    trade_dates = df_trade['trade_date'].unique()


    # my_sql = f"""SELECT T1.entry_date,T1.product_id, T2.ticker, T1.adj_price FROM product_market_data T1
    #                 JOIN product T2 on T1.product_id=T2.id WHERE entry_date='{last_date}';"""
    # df_end = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT product_id, adj_price FROM product_market_data WHERE (product_id, entry_date)
                 IN (SELECT product_id, MAX(entry_date) FROM product_market_data WHERE entry_date<'{end_date}'
                 GROUP BY product_id);"""
    df_end = pd.read_sql(my_sql, con=engine)

    for trade_date in trade_dates:
        my_sql = f"""SELECT T1.entry_date,T1.product_id, T2.ticker, T1.adj_price FROM product_market_data T1
                     JOIN product T2 on T1.product_id=T2.id WHERE entry_date='{trade_date}';"""
        df_start = pd.read_sql(my_sql, con=engine)

        df_temp = df_trade[df_trade['trade_date'] == trade_date]
        for index, row in df_temp.iterrows():
            product_id = row['product_id']
            # check if start price exist
            if not df_start[df_start['product_id'] == product_id].empty:
                start_price = df_start[df_start['product_id'] == product_id]['adj_price'].values[0]

                df_trade.loc[index, 'Px1'] = start_price
                if not df_end[df_end['product_id'] == product_id].empty:
                    end_price = df_end[df_end['product_id'] == product_id]['adj_price'].values[0]
                    df_trade.loc[index, 'Px2'] = end_price
                    perf = (end_price - start_price) / start_price
                    df_trade.loc[index, 'Perf'] = perf
                    df_trade.loc[index, 'Perf USD'] = perf * row['notional_usd']
                    df_trade.loc[index, 'PNL Total'] = row['pnl_close'] + df_trade.loc[index, 'Perf USD']
        print(trade_date)
    # save to csv
    # df_trade.to_csv(f'trade_emsx_pnl {end_date}x.csv', index=False)
    pnl = df_trade['PNL Total'].sum()
    new_trade_pnl = TradePnl(start_date=start_date,
                             end_date=end_date,
                             period='6M',
                             pnl=pnl)
    session.add(new_trade_pnl)
    session.commit()


def find_next_date(my_date):
    if my_date.weekday() == 4:
        previous_date = my_date + timedelta(days=3)
    else:
        previous_date = my_date + timedelta(days=1)
    return previous_date


if __name__ == '__main__':

    today = date.today()

    start_date = date(2019, 4, 1)
    end_date = date(2023, 6, 27)
    # get_full_pnl(start_date, end_date)
    # sys.exit()

    while end_date <= today:
        get_full_pnl(start_date, end_date)
        start_date = find_next_date(start_date)
        end_date = find_next_date(end_date)



