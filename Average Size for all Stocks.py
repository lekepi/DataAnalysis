from models import engine
import pandas as pd

if __name__ == '__main__':

    my_sql = """SELECT entry_date,T2.ticker,T1.mkt_value_usd FROM position T1 JOIN product T2 
    on T1.product_id=T2.id WHERE parent_fund_id=1 and entry_date>='2019-04-01' and prod_type='Cash' order by entry_date;"""
    df_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = "SELECT entry_date,long_usd FROM alpha_summary WHERE parent_fund_id=1 order by entry_date;"
    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # number of date
    n_date = len(df_long)

    # join
    df_pos = pd.merge(df_pos, df_long, on='entry_date', how='left')
    df_pos['expo'] = df_pos['mkt_value_usd'] / df_pos['long_usd']

    # group by ticker
    df_result = df_pos.groupby('ticker')['expo'].sum().reset_index()

    # sort by expo
    df_result = df_result.sort_values(by='expo', ascending=False)
    # divide by number of date
    df_result['expo'] = df_result['expo'] / n_date

    # export in excel folder
    df_result.to_excel(r'Excel\top_x_exposure.xlsx', index=False)
