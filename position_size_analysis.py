from models import engine
import pandas as pd


if __name__ == '__main__':
    my_sql = "SELECT entry_date,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE parent_fund_id=1 and prod_type='Cash' and quantity>0 and mkt_value_usd is not NULL;"
    df = pd.read_sql(my_sql, con=engine)

    my_sql = "SELECT entry_date,long_usd FROM anandaprod.alpha_summary WHERE parent_fund_id=1;"
    df2 = pd.read_sql(my_sql, con=engine)

    df = df.merge(df2, on='entry_date', how='left')
    df['size_perc'] = df['mkt_value_usd'] / df['long_usd'] * 100

    result = df.groupby('entry_date')['size_perc'].agg(['count', 'mean', 'std'])
    print(1)
