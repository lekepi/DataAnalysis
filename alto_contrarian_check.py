from models import engine
import pandas as pd
import numpy as np


if __name__ == '__main__':

    perf_type = '1y'  # ['1m', '3m', '6m', '1y', '2y', '3y']
    limit = -0.8  # -0.5

    my_sql = f"""SELECT entry_date,ticker,product_id,mkt_value_usd,beta,perf_{perf_type} FROM position WHERE parent_fund_id=1 
    and entry_date>='2019-04-01' and mkt_value_usd>0 and beta<>0 order by entry_date;"""
    df_pos = pd.read_sql(my_sql, con=engine)

    df_pos['region'] = np.where(df_pos['ticker'].str[-3:] == ' US', 'AMER',
                                np.where(df_pos['ticker'].str[-3:] == ' CN', 'AMER', 'EMEA'))

    # get SXXR Index (845) SPTR500N Index (916) price
    my_sql = f"""SELECT entry_date,perf_1y as sxxr FROM index_return WHERE product_id=845 and entry_date>='2019-04-01' order by entry_date;"""
    df_sxxr = pd.read_sql(my_sql, con=engine)

    my_sql = f"""SELECT entry_date,perf_1y as sptr FROM index_return WHERE product_id=916 and entry_date>='2019-04-01' order by entry_date;"""
    df_sptr = pd.read_sql(my_sql, con=engine)

    # merge df_pos with df_sxxr and df_sptr
    df_pos = pd.merge(df_pos, df_sxxr, on='entry_date', how='left')
    df_pos = pd.merge(df_pos, df_sptr, on='entry_date', how='left')

    # add perf_index column to df_pos
    df_pos['perf_index'] = np.where(df_pos['region'] == 'EMEA', df_pos['sxxr'], df_pos['sptr'])
    df_pos['alpha'] = df_pos['perf_' + perf_type] - df_pos['beta'] * df_pos['perf_index']

    df_alpha_neg = df_pos[df_pos['alpha'] < limit]
    df_alpha_sum = df_alpha_neg.groupby('entry_date')['mkt_value_usd'].sum().reset_index()
    df_alpha_sum.columns = ['entry_date', 'sum_condition_usd']

    my_sql = """SELECT entry_date,long_usd FROM alpha_summary WHERE entry_date>='2019-04-01' and parent_fund_id=1;"""
    df_long = pd.read_sql(my_sql, con=engine)
    df_result = pd.merge(df_long, df_alpha_sum, on='entry_date', how='left')
    # fill na with 0
    df_result['sum_condition_usd'] = df_result['sum_condition_usd'].fillna(0)
    df_result['perc'] = df_result['sum_condition_usd'] / df_result['long_usd']

    df_today = df_pos[df_pos['entry_date'] == df_pos['entry_date'].max()]
    # join with df_long
    df_today = pd.merge(df_today, df_long, on='entry_date', how='left')
    df_today['perc'] = df_today['mkt_value_usd'] / df_today['long_usd']

    # save in /excel folder
    df_result.to_excel(f'excel/Alto Contrarian {perf_type}.xlsx', index=False)
    df_today.to_excel(f'excel/Alto Contrarian {perf_type} Today.xlsx', index=False)

    pass



