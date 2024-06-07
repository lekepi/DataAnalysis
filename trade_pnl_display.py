from models import engine
import pandas as pd


if __name__ == '__main__':
    my_sql = "select start_date,end_date,pnl from trade_pnl"
    df = pd.read_sql(my_sql, con=engine)

    my_sql = "select entry_date as start_date,amount*100/deployed as aum from aum where entry_date>='2019-04-01' and type='leveraged' and fund_id=4"
    df_aum = pd.read_sql(my_sql, con=engine)

    # left join
    df = pd.merge(df, df_aum, how='left', left_on='start_date', right_on='start_date')

    # fill aum column with previous value
    df['aum'] = df['aum'].fillna(method='ffill')
    df['perf'] = df['pnl'] / (df['aum'] *1000000)
    #csv
    df.to_csv('trade_pnl.csv', index=False)
    print(df)

