import pandas as pd
from models import engine
from datetime import date, timedelta


def get_alto_long_perf(my_date, previous_date):

    # get alto_position
    my_sql = f"""SELECT T2.ticker,mkt_value_usd,T5.code as cncy FROM position T1 
    JOIN product T2 on T1.product_id=T2.id JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on 
    T3.country_id=T4.id JOIN currency T5 on T2.currency_id=T5.id WHERE entry_date='{my_date}' and 
    parent_fund_id=1 and prod_type='Cash' and continent='EMEA' and quantity>0;"""
    df_position = pd.read_sql(my_sql, con=engine)

    # get previous fx
    my_sql = f"""SELECT code as cncy,rate as previous_rate FROM currency_history T1 
    JOIN currency T2 on T1.currency_id=T2.id WHERE entry_date='{previous_date}';"""
    df_previous_fx = pd.read_sql(my_sql, con=engine)
    prev_eur_rate = df_previous_fx[df_previous_fx['cncy'] == 'EUR']['previous_rate'].values[0]

    # get current fx
    my_sql = f"""SELECT code as cncy,rate as current_rate FROM currency_history T1
    JOIN currency T2 on T1.currency_id=T2.id WHERE entry_date='{my_date}';"""
    df_current_fx = pd.read_sql(my_sql, con=engine)
    eur_rate = df_current_fx[df_current_fx['cncy'] == 'EUR']['current_rate'].values[0]

    # merge df_position with previous and current fx
    df_position = df_position.merge(df_previous_fx, on='cncy', how='left')
    df_position = df_position.merge(df_current_fx, on='cncy', how='left')

    my_sql = f"""SELECT ticker,return_1d FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE entry_date='{my_date}';"""
    df_perf = pd.read_sql(my_sql, con=engine)

    # merge df_position with performance
    df_position = df_position.merge(df_perf, on='ticker', how='left')

    df_position['mkt_value_eur'] = df_position['mkt_value_usd'] * prev_eur_rate
    df_position['mkt_value_local'] = df_position['mkt_value_usd'] * df_position['previous_rate']
    df_position['pnl_local'] = df_position['mkt_value_local'] * df_position['return_1d']
    df_position['pnl_eur'] = df_position['pnl_local'] / df_position['current_rate'] * eur_rate

    total_mkt_value_eur = df_position['mkt_value_eur'].sum()
    total_pnl_eur = df_position['pnl_eur'].sum()
    perf_eur = total_pnl_eur / total_mkt_value_eur if total_mkt_value_eur != 0 else 0
    print(f"ALTO EMEA -  {my_date}")
    return perf_eur, total_mkt_value_eur, total_pnl_eur


if __name__ == '__main__':

    df_result = pd.DataFrame(columns=['date', 'perf_eur', 'total_mkt_value_eur', 'total_pnl_eur'])

    previous_date = date(2019, 3, 29)
    my_date = date(2019, 4, 1)
    day = timedelta(days=1)
    while my_date < date.today():
        week_num = my_date.weekday()
        if week_num < 5:  # ignore Weekend
            perf_eur, total_mkt_value_eur, total_pnl_eur = get_alto_long_perf(my_date, previous_date)
            df_result = df_result._append({'date': my_date,
                                          'perf_eur': perf_eur,
                                          'total_mkt_value_eur': total_mkt_value_eur,
                                          'total_pnl_eur': total_pnl_eur
                                          }, ignore_index=True)
            previous_date = my_date
        my_date += day
    # save into excel in Excel folder
    df_result.to_excel('Excel/Alto_EMEA_Perf.xlsx', index=False)