import pandas as pd
from models import engine
from utils import last_alpha_date
from datetime import date, timedelta
import openpyxl


def method_1():
    end_date = date(2026, 4, 17)
    start_date = date(2026, 4, 17) - timedelta(days=7*52*3)


    my_sql = f"""SELECT product_id FROM product_group_analyst T1 WHERE group_name='Kennelinc4';"""
    df = pd.read_sql(my_sql, con=engine)
    product_list = df['product_id'].tolist()
    product_id_sql = str(tuple(product_list))

    my_sql = f"""SELECT entry_date,product_id,adj_price from product_market_data where entry_date>='{start_date}' and 
    entry_date<='{end_date}' and product_id in {product_id_sql} order by entry_date;"""
    df_price = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # pivot
    df_price_pivot = df_price.pivot_table(values='adj_price', index=df_price.index, columns='product_id')
    # fill missing value with previous
    df_price_pivot.fillna(method='ffill', inplace=True)

    df_daily_return = df_price_pivot.pct_change()
    df_daily_return['avg_daily_return'] = df_daily_return.mean(axis=1)
    df_daily_return['cum_daily_return'] = (1 + df_daily_return['avg_daily_return'].fillna(0)).cumprod() - 1

    my_date_list = [date(2026, 4, 17) - timedelta(days=7*52*i) for i in range(4)]
    df_price_pivot_filtered = df_price_pivot[df_price_pivot.index.isin(my_date_list)]
    df_yearly_return = df_price_pivot_filtered.pct_change()
    df_yearly_return['avg_annual_return'] = df_yearly_return.mean(axis=1)
    df_yearly_return['cum_annual_return'] = (1 + df_yearly_return['avg_annual_return'].fillna(0)).cumprod() - 1

    # merge daily return and yearly return
    df_return = pd.merge(df_daily_return[['avg_daily_return', 'cum_daily_return']], df_yearly_return[['avg_annual_return', 'cum_annual_return']], left_index=True, right_index=True, how='outer')


def method_2():
    end_date = date(2026, 4, 17)
    start_date = date(2026, 4, 17) - timedelta(days=7*52*3)
    my_date_list = [date(2026, 4, 17) - timedelta(days=7 * 52 * i) for i in range(4)]

    my_sql = f"""SELECT product_id FROM product_group_analyst T1 WHERE group_name='Kennelinc4';"""
    df = pd.read_sql(my_sql, con=engine)
    product_list = df['product_id'].tolist()
    product_id_sql = str(tuple(product_list))

    my_sql = f"""SELECT entry_date,product_id,adj_price from product_market_data where entry_date>='{start_date}' and 
    entry_date<='{end_date}' and product_id in {product_id_sql} order by entry_date;"""
    df_price = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # pivot
    df_price = df_price.pivot_table(values='adj_price', index=df_price.index, columns='product_id')
    # fill missing value with previous
    df_price.fillna(method='ffill', inplace=True)
    df_returns = df_price.pct_change()

    # 1. Get the prices only for the dates in your rebalance list
    # We filter the df for only those dates that actually exist in the index
    rebalance_prices = df_price.loc[df_price.index.isin(my_date_list)]

    # 2. Reindex these prices to the full df_price index
    # This creates a dataframe of the same shape as df_price,
    # filled with NaNs except on the rebalance dates.
    df_base_prices = rebalance_prices.reindex(df_price.index).ffill()

    # 3. Calculate returns and average
    df_yearly_returns = (df_price / df_base_prices) - 1

    df_price['daily_return'] = df_returns.mean(axis=1)
    df_price['yearly_return'] = df_yearly_returns.mean(axis=1)



    print(1)


if __name__ == '__main__':
    method_2()


