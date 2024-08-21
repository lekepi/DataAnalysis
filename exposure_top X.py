from models import engine
import pandas as pd

if __name__ == '__main__':

    top_num = 10
    my_type = 'Short'

    my_sql = """SELECT entry_date,T2.ticker,mkt_value_usd,prod_type FROM anandaprod.position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE parent_fund_id=1 AND entry_date>='2019-04-01' order by entry_date,mkt_value_usd desc;"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    if my_type == 'Long':

        # get sum of mkt_value_usd group by entry_date when mkt_value_usd>0
        df_sum = df[df['mkt_value_usd']>0].groupby(['entry_date'])['mkt_value_usd'].sum()

        # get the sum of the top X mkk_value_usd group by entry_date
        df_sum_topX = df.groupby(['entry_date'])['mkt_value_usd'].apply(lambda x: x.nlargest(top_num).sum())

        # merge the two dataframes
        df = pd.merge(df_sum, df_sum_topX, on=['entry_date'], how='left')
        df['percent'] = df['mkt_value_usd_y'] / df['mkt_value_usd_x'] * 100

    else:
        # get sum of mkt_value_usd group by entry_date when mkt_value_usd<0
        df_sum = df[df['mkt_value_usd']<0].groupby(['entry_date'])['mkt_value_usd'].sum()

        # keep prod_type='Cash' only
        df = df[df['prod_type'] == 'Cash']

        # get the sum of the top X mkk_value_usd group by entry_date
        df_sum_topX = df.groupby(['entry_date'])['mkt_value_usd'].apply(lambda x: x.nsmallest(top_num).sum())

        # merge the two dataframes
        df = pd.merge(df_sum, df_sum_topX, on=['entry_date'], how='left')
        df['percent'] = df['mkt_value_usd_y'] / df['mkt_value_usd_x'] * 100

        pass
    pass

