from models import engine
import pandas as pd
from datetime import date


def create_alto_position(my_date, region):

    if region == 'AMER-EMEA':
        sql_region = ""
        sql_fund = 'ALTO_ALL'
    elif region == 'AMER':
        sql_region = " and T4.continent='AMER'"
        sql_fund = 'ALTO_AMER'
    else:
        sql_region = " and T4.continent!='AMER'"
        sql_fund = 'ALTO_EMEA'

    my_sql = f"""SELECT T2.ticker as Ticker,quantity as Qty,'{sql_fund}' as Fund,entry_date as Date from position 
    T1 JOIN product T2 on T1.product_id=T2.id
    JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
     WHERE quantity<>0 and parent_fund_id=1 and entry_date='{my_date}' and prod_type='Cash' {sql_region};"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['Date'])
    # convert the date into a string with format "YYYYMMDD"
    df['Date'] = df['Date'].dt.strftime('%Y%m%d')

    # save df into excel xlsx file
    df.to_excel(rf'H:\Factors\BBU\Upload\{region}.xlsx', index=False)


if __name__ == '__main__':

    my_date = date.today()

    create_alto_position(my_date, 'AMER-EMEA')
    create_alto_position(my_date, 'AMER')
    create_alto_position(my_date, 'EMEA')
