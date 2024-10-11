import pandas as pd
from models import engine


if __name__ == '__main__':
    my_sql = "SELECT entry_date,long_usd FROM alpha_summary WHERE parent_fund_id=1;"
    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = """SELECT entry_date,T2.ticker,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE parent_fund_id=1 and quantity>0 and prod_type='Cash' and entry_date>'2019-04-01' order by ticker,entry_date;"""

    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    #join the two dataframes
    df = pd.merge(df_position, df_long, on=['entry_date'], how='left')
    df['percent'] = df['mkt_value_usd'] / df['long_usd']

    # sort by percent decending
    df_sort = df.sort_values(by=['percent'], ascending=False)
    # keep the top 10 lines with different tickers
    df_sort = df_sort.drop_duplicates(subset=['ticker'])
    # keep only the first 20 lines
    df_sort = df_sort.head(20)

    # from df keep only ticker and percent columns
    df = df[['ticker', 'percent']]
    # from df, sum percentage group by ticker
    df_sum = df.groupby(['ticker']).sum()
    # keep only the percent column
    df_sum = df_sum[['percent']]
    df_sum['percent'] = df_sum['percent'] / len(df_long)
    # sort by percent decending
    df_sum = df_sum.sort_values(by=['percent'], ascending=False)
    # keep only the first 20 lines
    df_sum = df_sum.head(20)

    my_sql = "Select ticker,name from product"
    df_name = pd.read_sql(my_sql, con=engine)
    df_sum = pd.merge(df_sum, df_name, on=['ticker'], how='left')
    df_sum.to_excel(r'Excel\average_large_position.xlsx', index=True)

    print(1)
