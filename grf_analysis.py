import pandas as pd
from models import engine


if __name__ == '__main__':

    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as notional_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and parent_fund_id=1 and entry_date>='2019-04-01' group by T1.entry_date
        Order by T1.entry_date;"""

    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = f"""SELECT entry_date,beta,alpha,return_1d FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id 
                 WHERE T2.ticker='GRF SM' and entry_date>='2019-04-01';"""
    df_beta = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = f"""SELECT entry_date,mkt_value_usd as pos_usd,alpha_usd as alpha_usd_pos,perf_1d as return_1d_pos,pnl_usd as pnl_usd_pos,beta as beta_pos FROM position T1 JOIN product T2 
    on T1.product_id=T2.id WHERE parent_fund_id=1 and T2.ticker='GRF SM' and entry_date>='2019-04-01';"""

    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = f"""SELECT entry_date,perf_1d as sxxr_perf FROM index_return T1 JOIN product T2 on T1.product_id=T2.id
    WHERE T2.ticker='SXXR Index' and entry_date>='2019-04-01';"""
    df_index = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = f"""SELECT entry_date,perf_1d as sxxp_perf FROM index_return T1 JOIN product T2 on T1.product_id=T2.id
    WHERE T2.ticker='SXXP Index' and entry_date>='2019-04-01';"""
    df_index2 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    df = pd.concat([df_long, df_beta, df_position, df_index, df_index2], axis=1)
    # complete the missing values with 0
    df.fillna(0, inplace=True)
    df['alpha_alto'] = df['alpha_usd_pos'] / df['notional_usd']*10000
    df['alpha_alto_cum'] = df['alpha_alto'].cumsum()
    # add column beta 2 that is max(beta, 0.8)

    df['beta_2'] = df['beta']
    # df['beta_2'] = df['beta'].apply(lambda x: max(x, 0.5))
    mask = df.index > '2022-01-01'
    df.loc[mask, 'beta_2'] = df.loc[mask, 'beta'].apply(lambda x: max(x, 0.8))

    df['alpha_sxxr'] = df['return_1d'] - df['beta_2'] * df['sxxr_perf']
    df['alpha_sxxr_usd'] = df['alpha_sxxr'] * df['pos_usd']
    df['alpha_sxxr_alto'] = df['alpha_sxxr_usd'] / df['notional_usd']*10000
    df['alpha_sxxr_alto_cum'] = df['alpha_sxxr_alto'].cumsum()

    df['alpha_sxxp'] = df['return_1d'] - df['beta_2'] * df['sxxp_perf']
    df['alpha_sxxp_usd'] = df['alpha_sxxp'] * df['pos_usd']
    df['alpha_sxxp_alto'] = df['alpha_sxxp_usd'] / df['notional_usd']*10000
    df['alpha_sxxp_alto_cum'] = df['alpha_sxxp_alto'].cumsum()

    print(1)



