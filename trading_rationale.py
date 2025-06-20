from datetime import date, timedelta
import pandas as pd
from models import engine,session, Product, Trade


product_db = session.query(Product).all()


def run_trade_rationale(entry_date):


    my_sql = f"""SELECT T2.ticker,mkt_value_usd as position_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE parent_fund_id=1 and entry_date='{entry_date}';"""
    df_position = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT quantity,T2.ticker,prod_type,notional_usd as trade_usd,exec_price FROM trade T1 JOIN product T2 
    on T1.product_id=T2.id WHERE trade_date='{entry_date}' and parent_fund_id=1 and prod_type='Cash'"""
    df_trade = pd.read_sql(my_sql, con=engine)

    # merge df_trade and df_position
    df_trade = df_trade.merge(df_position, on='ticker', how='left')

    my_sql = f"""SELECT T2.ticker,MIN(trade_date) as first_date FROM trade T1 JOIN product T2 on T1.product_id=T2.id
             WHERE T1.parent_fund_id=1 GROUP BY T2.ticker HAVING first_date ='{entry_date}'"""
    df_first = pd.read_sql(my_sql, con=engine)

    df_trade = df_trade.merge(df_first, on='ticker', how='left')

    df_trade['rationale'] = 0

    # replace all nan in df_trade by NONE
    df_trade = df_trade.fillna(0)
    df_trade['perc'] = (df_trade['trade_usd'] / df_trade['position_usd']).abs() * 100

    # rationale = 1 if first_date <> 0
    df_trade.loc[df_trade['first_date'] != 0, 'rationale'] = 1

    # rationale =1 if perc>30 and abs(trade_usd)>500000
    df_trade.loc[(df_trade['perc'] > 30) & (df_trade['trade_usd'].abs() > 500000), 'rationale'] = 1

    df_trade_rationale = df_trade[df_trade['rationale'] == 1]
    df_trade_rationale['trade_date'] = entry_date
    return df_trade_rationale


if __name__ == '__main__':

    df_result = pd.DataFrame()
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
        run_trade_rationale(my_date)
    elif my_mode == "SpecificDay":
        my_date = date(2025, 4, 1)
        run_trade_rationale(my_date)
    else:  # Range / Loop
        my_date = date(2025, 1, 1)
        day = timedelta(days=1)
        while my_date < date.today():
            week_num = my_date.weekday()
            if week_num < 5:  # ignore Weekend
                df = run_trade_rationale(my_date)
                if df_result.empty:
                    df_result = df
                else:
                    df_result = pd.concat([df_result, df], ignore_index=True)
            my_date += day
        # save into excel folder, not normal folder
        df_result.to_excel('Excel\\trade_rationale.xlsx', index=False)


