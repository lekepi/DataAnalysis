from datetime import date
from models import engine
import pandas as pd


def get_data():
    start_date = date(2019, 4, 1)
    end_date = date.today()
    fund_id = 1
    my_type = 'alpha'
    ticker_sql = ""
    is_intraday = True

    # Long notional USD
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as notional_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and T1.parent_fund_id={fund_id} and entry_date>='{start_date}' and entry_date<='{end_date}' group by T1.entry_date
        Order by T1.entry_date;"""
    df_alto_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # amer Long notional USD
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as amer_notional_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id LEFT JOIN exchange T3 on T2.exchange_id=T3.id 
        LEFT JOIN country T4 on T3.country_id=T4.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and T4.continent='AMER' and T1.parent_fund_id={fund_id} and entry_date>='{start_date}' 
        and entry_date<='{end_date}' group by T1.entry_date Order by T1.entry_date;"""
    df_alto_amer_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # alpha/PNL USD
    my_sql = f"""SELECT T1.entry_date,sum(T1.{my_type}_usd) as usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type in ('Cash','Future') 
        and T1.quantity<>0 and T1.parent_fund_id={fund_id} and entry_date>='{start_date}' and
         entry_date<='{end_date}' {ticker_sql} group by T1.entry_date
        Order by T1.entry_date;"""
    df_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # Alpha/PNL Long
    my_sql = f"""SELECT T1.entry_date, sum(T1.{my_type}_usd) as long_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type in ('Cash','Future')  
        and T1.parent_fund_id={fund_id} and T1.quantity>0 and entry_date>='{start_date}' and
         entry_date<='{end_date}' {ticker_sql} group by T1.entry_date Order by T1.entry_date;"""
    df_long_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # Alpha/PNL amer
    my_sql = f"""SELECT T1.entry_date, sum(T1.{my_type}_usd) as amer_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id 
        LEFT JOIN exchange T3 on T2.exchange_id=T3.id LEFT JOIN country T4 on T3.country_id=T4.id 
        WHERE T2.prod_type in ('Cash','Future') and T4.continent='AMER' and T1.parent_fund_id={fund_id} 
        and T1.quantity<>0 and entry_date>='{start_date}' and entry_date<='{end_date}' {ticker_sql} group by T1.entry_date
        Order by T1.entry_date;"""
    df_amer_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # Alpha/PNL short amer
    my_sql = f"""SELECT T1.entry_date, sum(T1.{my_type}_usd) as short_amer_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id 
        LEFT JOIN exchange T3 on T2.exchange_id=T3.id LEFT JOIN country T4 on T3.country_id=T4.id 
        WHERE T2.prod_type in ('Cash','Future') and T4.continent='AMER' and T1.quantity<0 and T1.parent_fund_id={fund_id} 
        and T1.quantity<>0 and entry_date>='{start_date}' and entry_date<='{end_date}' {ticker_sql} group by T1.entry_date
        Order by T1.entry_date;"""
    df_short_amer_usd = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    if is_intraday:
        my_sql = f"""SELECT T1.trade_date as entry_date,sum(T1.pnl_close) as intraday_pnl FROM trade T1 JOIN product T2 on T1.product_id=T2.id 
                     WHERE T1.parent_fund_id={fund_id} and trade_date>='{start_date}' and 
                     trade_date<='{end_date}' and T2.prod_type not in ('Call', 'Put', 'Roll') group by T1.trade_date order by T1.trade_date;"""
        df_intraday_pnl = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
        df_intraday_pnl['Intraday PNL'] = df_intraday_pnl['intraday_pnl'].cumsum()

    # alpha vs long notional
    df_alpha_perf = pd.concat([df_usd, df_long_usd, df_amer_usd, df_short_amer_usd, df_alto_usd, df_alto_amer_usd], axis=1)
    df_alpha_perf['all_usd'] = df_alpha_perf['usd']

    if is_intraday:
        df_alpha_perf = pd.concat([df_alpha_perf, df_intraday_pnl], axis=1)
        # fill na with 0
        df_alpha_perf['intraday_pnl'] = df_alpha_perf['intraday_pnl'].fillna(0)
        df_alpha_perf['all_usd'] = df_alpha_perf['all_usd'] + df_alpha_perf['intraday_pnl']

    df_alpha_perf['short_usd'] = df_alpha_perf['usd'] - df_alpha_perf['long_usd']
    df_alpha_perf['emea_usd'] = df_alpha_perf['usd'] - df_alpha_perf['amer_usd']
    df_alpha_perf['emea_notional_usd'] = df_alpha_perf['notional_usd'] - df_alpha_perf['amer_notional_usd']
    df_alpha_perf['short_emea_usd'] = df_alpha_perf['short_usd'] - df_alpha_perf['short_amer_usd']
    df_alpha_perf['perc'] = df_alpha_perf['all_usd'] / df_alpha_perf['notional_usd']
    df_alpha_perf['perc_long'] = df_alpha_perf['long_usd'] / df_alpha_perf['notional_usd']
    df_alpha_perf['perc_short'] = df_alpha_perf['short_usd'] / df_alpha_perf['notional_usd']
    df_alpha_perf['perc_amer'] = df_alpha_perf['amer_usd'] / df_alpha_perf['amer_notional_usd']
    df_alpha_perf['perc_emea'] = df_alpha_perf['emea_usd'] / df_alpha_perf['emea_notional_usd']
    df_alpha_perf['perc_short_amer'] = df_alpha_perf['short_amer_usd'] / df_alpha_perf['amer_notional_usd']
    df_alpha_perf['perc_short_emea'] = df_alpha_perf['short_emea_usd'] / df_alpha_perf['emea_notional_usd']

    # keep only columns that contain perc
    df_alpha_perf = df_alpha_perf[[col for col in df_alpha_perf.columns if 'perc' in col]]
    # replace 'perc' per 'alpha'
    df_alpha_perf.columns = [col.replace('perc', 'alpha') for col in df_alpha_perf.columns]

    for col in df_alpha_perf.columns:
        df_alpha_perf[col + "_1y_rol"] = df_alpha_perf[col].rolling(262).sum()
    # save df_alpha_perf into excel
    df_alpha_perf.to_excel(rf'H:/Python Output/Investor Report/alpha_perf_{end_date}.xlsx')


if __name__ == '__main__':

    get_data()
