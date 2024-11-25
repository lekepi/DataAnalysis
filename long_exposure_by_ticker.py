from datetime import date
import pandas as pd
from models import engine


def get_long_exposure_by_ticker(start_date, end_date, long_number, frequency):
    my_sql = f"""SELECT YEAR(entry_date) AS year,MONTH(entry_date) AS month,MAX(entry_date) AS max_date
                   FROM position WHERE entry_date>='{start_date}' and entry_date<='{end_date}' 
                   GROUP BY YEAR(entry_date), MONTH(entry_date) ORDER BY year, month;"""
    df_date = pd.read_sql(my_sql, con=engine)
    df_date = df_date[:-1]
    max_date_list = df_date['max_date'].tolist()
    max_date_list.insert(0, date(2019, 4, 1))
    # get max_date_list in one string separated by comma
    max_date_list_str = ",".join(["'" + str(x) + "'" for x in max_date_list])

    my_sql = f"""SELECT entry_date,T2.ticker,T2.isin,T2.sedol,T2.name,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
        WHERE T1.parent_fund_id=1 and entry_date in ({max_date_list_str}) and quantity>0 
        and prod_type='Cash' and T2.ticker not in ('AGI US', 'FNV US','FNV CN','NEM US','GOLD US','AEM US','GDX US','GC1 CMX','GLD US') 
        order by entry_date,mkt_value_usd desc;"""
    df_pos = pd.read_sql(my_sql, con=engine)

    if long_number:
        # keep only top long_number each day
        df_pos = df_pos.groupby('entry_date').head(long_number)

    # get sum mkt_value_usd when >0 grouped by entry_date
    df_long = df_pos.groupby('entry_date')['mkt_value_usd'].sum().reset_index()
    # rename mkt_value_usd to long_usd
    df_long.rename(columns={'mkt_value_usd': 'long_usd'}, inplace=True)
    # merge
    df_pos = df_pos.merge(df_long, on='entry_date', how='left')
    df_pos['weight'] = df_pos['mkt_value_usd'] / df_pos['long_usd']

    # save in excel folder
    df_pos.to_excel(r'Excel\long_exposure_by_ticker.xlsx', index=False)


if __name__ == '__main__':

    start_date = date(2019, 4, 1)
    end_date = date.today()
    frequency = 'monthly'  # daily, weekly, monthly, quarterly, yearly
    long_number = None  # 10, 20, 40, None

    get_long_exposure_by_ticker(start_date, end_date, long_number, frequency)