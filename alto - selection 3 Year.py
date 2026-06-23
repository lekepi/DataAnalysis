from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from models import session, engine
import pandas as pd


def last_business_day_of_month(my_date):
    # Step 1: Get the last day of the month
    if my_date.month == 12:
        next_month = my_date.replace(year=my_date.year + 1, month=1, day=1)
    else:
        next_month = my_date.replace(month=my_date.month + 1, day=1)
    last_day = next_month - timedelta(days=1)

    # Step 2: Adjust if it's a weekend
    if last_day.weekday() == 5:  # Saturday
        last_day -= timedelta(days=1)
    elif last_day.weekday() == 6:  # Sunday
        last_day -= timedelta(days=2)

    return last_day


def get_selection_perf(my_date, product_id_str, df_stock_index, df_perf_universe):

    date_3y = my_date + timedelta(days=1092)

    # Get all the stock for that date in alto
    my_sql = f"""SELECT product_id FROM position WHERE parent_fund_id=1 and entry_date='{my_date}' and quantity>0
    and product_id in ({product_id_str});"""
    df_alto_stock = pd.read_sql(my_sql, con=engine)

    # get list of product_id in a string separated by comma
    product_id_alto_list = df_alto_stock['product_id'].tolist()
    product_id_alto_str = ",".join(map(str, product_id_alto_list))

    my_sql = f"""SELECT product_id,entry_date,adj_price FROM product_market_data WHERE entry_date in ('{my_date}','{date_3y}')
     and product_id in ({product_id_alto_str},845,916);"""
    df_market_data = pd.read_sql(my_sql, con=engine)
    # pivot df:
    df_market_data = df_market_data.pivot(index='product_id', columns='entry_date', values='adj_price').reset_index()
    df_market_data['perf'] = (df_market_data[date_3y] - df_market_data[my_date]) / df_market_data[my_date] * 100

    amer_perf = df_market_data[df_market_data['product_id'] == 916]['perf'].values[0]
    europe_perf = df_market_data[df_market_data['product_id'] == 845]['perf'].values[0]

    # get the performance of the universe for that date
    amer_univ_perf = df_perf_universe.loc[my_date, 'AMER_3y'] * 100
    europe_univ_perf = df_perf_universe.loc[my_date, 'EMEA_3y'] * 100

    # join with df_alto_stock
    df_alto_stock = df_alto_stock.merge(df_market_data, on='product_id', how='left')
    # join with df_stock_index
    df_alto_stock = df_alto_stock.merge(df_stock_index[['id', 'index_id']], left_on='product_id', right_on='id', how='left')

    # add index perf based on index_id
    df_alto_stock['index_perf'] = df_alto_stock['index_id'].apply(lambda x: amer_perf if x == 916 else europe_perf)
    df_alto_stock['univ_perf'] = df_alto_stock['index_id'].apply(lambda x: amer_univ_perf if x == 916 else europe_univ_perf)

    df_alto_stock['relative_perf'] = df_alto_stock['perf'] - df_alto_stock['index_perf']
    df_alto_stock['relative_univ_perf'] = df_alto_stock['perf'] - df_alto_stock['univ_perf']

    positive_perf_count = df_alto_stock[df_alto_stock['relative_perf'] > 0].shape[0]
    negative_perf_count = df_alto_stock[df_alto_stock['relative_perf'] < 0].shape[0]
    hit_ratio_perf = positive_perf_count / (positive_perf_count + negative_perf_count) if (positive_perf_count + negative_perf_count) > 0 else 0
    avg_perf = df_alto_stock['relative_perf'].mean()

    positive_univ_count = df_alto_stock[df_alto_stock['relative_univ_perf'] > 0].shape[0]
    negative_univ_count = df_alto_stock[df_alto_stock['relative_univ_perf'] < 0].shape[0]
    hit_ratio_univ = positive_univ_count / (positive_univ_count + negative_univ_count) if (positive_univ_count + negative_univ_count) > 0 else 0
    avg_univ = df_alto_stock['relative_univ_perf'].mean()

    # alpha and perf calculation
    my_sql = f"SELECT entry_date,product_id,alpha from product_beta where entry_date>='{my_date}' and entry_date<='{date_3y}' and product_id in ({product_id_alto_str})"
    df_alpha = pd.read_sql(my_sql, con=engine)
    df_alpha = df_alpha.pivot(index='entry_date', columns='product_id', values='alpha').reset_index()

    # New df with index=product_id, one column with sum of alpha
    df_alpha_sum = df_alpha.drop(columns='entry_date').sum().reset_index()
    df_alpha_sum.columns = ['product_id', 'sum_alpha']

    positive_alpha_count = df_alpha_sum[df_alpha_sum['sum_alpha'] > 0].shape[0]
    negative_alpha_count = df_alpha_sum[df_alpha_sum['sum_alpha'] < 0].shape[0]
    hit_ratio_alpha = positive_alpha_count / (positive_alpha_count + negative_alpha_count) if (positive_alpha_count + negative_alpha_count) > 0 else 0
    avg_alpha = df_alpha_sum['sum_alpha'].mean()

    print(my_date)
    return positive_perf_count, negative_perf_count, hit_ratio_perf, avg_perf, \
           positive_alpha_count, negative_alpha_count, hit_ratio_alpha, avg_alpha, \
           positive_univ_count, negative_univ_count, hit_ratio_univ, avg_univ


if __name__ == "__main__":

    analyst = 'Samson'

    if analyst == 'Araceli':
        user_id = 2
    elif analyst == 'Samson':
        user_id = 3
    elif analyst == 'Alex':
        user_id = 4

    # from excel/universe_3y.xlsx get df
    df_perf_universe = pd.read_excel(r'excel\universe_3y perf.xlsx')
    df_perf_universe = df_perf_universe[df_perf_universe['AMER_3y'].notna()]
    df_perf_universe = df_perf_universe[['Date', 'AMER_3y', 'EMEA_3y']]
    # convert date a s date
    df_perf_universe.set_index('Date', inplace=True)
    df_perf_universe.index = pd.to_datetime(df_perf_universe.index).date

    my_sql = "SELECT T1.id, continent FROM product T1 JOIN exchange T2 on T1.exchange_id=T2.id JOIN country T3 on T2.country_id=T3.id WHERE prod_type='Cash'"
    df_stock_index = pd.read_sql(my_sql, con=engine)

    #SXXR: 845, SPTR500N: 916
    # if continent==AMER, df_stock_index['index_id'] = 916 else 845
    df_stock_index['index_id'] = df_stock_index.apply(lambda row: 916 if row['continent'] == 'AMER' else 845, axis=1)

    my_sql = f"""SELECT product_id,ticker FROM analyst_universe T1 join product T2 on T1.product_id=T2.id 
    WHERE user_id={user_id} and end_date is NULL and priority=1"""

    df_universe = pd.read_sql(my_sql, con=engine)
    # get all the ids in a string for sql
    df_universe['product_id'] = df_universe['product_id'].astype(str)
    product_id_str = ','.join(df_universe['product_id'].astype(str))

    #send list of ticker with their location too to know the index

    first_date = date(2019, 4, 1)
    last_date = date.today() - relativedelta(years=3)

    df_result = pd.DataFrame(columns=['date', 'positive_perf_count', 'negative_perf_count', 'hit_ratio_perf', 'avg_perf',
                                      'positive_alpha_count', 'negative_alpha_count', 'hit_ratio_alpha', 'avg_alpha'])

    my_date = first_date
    while my_date <= last_date:
        my_date = last_business_day_of_month(my_date)
        positive_perf_count, negative_perf_count, hit_ratio_perf, avg_perf, \
        positive_alpha_count, negative_alpha_count, hit_ratio_alpha, avg_alpha, \
        positive_univ_count, negative_univ_count, hit_ratio_univ, avg_univ \
            = get_selection_perf(my_date, product_id_str, df_stock_index, df_perf_universe)
        df_result = df_result._append({
            'date': my_date,
            'positive_perf_count': positive_perf_count,
            'negative_perf_count': negative_perf_count,
            'count': positive_perf_count + negative_perf_count,
            'hit_ratio_perf': hit_ratio_perf,
            'avg_perf': avg_perf,
            'positive_alpha_count': positive_alpha_count,
            'negative_alpha_count': negative_alpha_count,
            'hit_ratio_alpha': hit_ratio_alpha,
            'avg_alpha': avg_alpha,
            'positive_univ_count': positive_univ_count,
            'negative_univ_count': negative_univ_count,
            'hit_ratio_univ': hit_ratio_univ,
            'avg_univ': avg_univ

        }, ignore_index=True)

        my_date = my_date + timedelta(days=7)  # Move to the next month
    # save into excel:
    df_result.to_excel(f'Excel\selection_3y_perf_{analyst}.xlsx', index=False)

