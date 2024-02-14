import pandas as pd
from models import engine


def get_var():
    my_sql = """SELECT entry_date,data_daily as alto FROM nav_account_statement WHERE status='Daily' and 
    entry_date>='2019-04-01' and data_name='RETURN USD CLASS L' and active=1 order by entry_date;"""
    df_alto = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = """SELECT entry_date,perf_1d as spx FROM index_return T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE T2.ticker='SPTR500N Index' and entry_date>='2019-04-01';"""
    df_spx = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = """SELECT entry_date,perf_1d as sxxp FROM index_return T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE T2.ticker='SXXR Index' and entry_date>='2019-04-01';"""
    df_sxxp = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    df = pd.concat([df_alto, df_spx, df_sxxp], axis=1)
    pass


if __name__ == '__main__':
    get_var()
