from datetime import date, timedelta
import pandas as pd
from models import engine


def find_next_date(my_date):
    if my_date.weekday() == 4:
        previous_date = my_date + timedelta(days=3)
    else:
        previous_date = my_date + timedelta(days=1)
    return previous_date


def assign_region(ticker):
    if ticker[-3:-1] == ' U' or ticker[-3:-1] == ' T':
        return 'AMER'
    else:
        return 'EMEA'


def get_univ_rel_perf(my_date, next_date):

    my_sql = f"""SELECT product_id FROM product_universe WHERE (start_date='2019-04-01' or start_date<{my_date}) 
    and (end_date is NULL or end_date>{my_date});"""
    df = pd.read_sql(my_sql, con=engine)
    product_list = df['product_id'].tolist()
    product_sql = ','.join([str(x) for x in product_list])

    # add product id 845 and 916
    product_sql += ',845,916'

    my_sql = f"""SELECT entry_date,ticker,adj_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id
    WHERE entry_date in ('{my_date}','{next_date}') and product_id in ({product_sql}) order by entry_date;"""

    df = pd.read_sql(my_sql, con=engine)
    # pivot table
    df = df.pivot(index='ticker', columns='entry_date', values='adj_price')
    df['return'] = df[next_date] / df[my_date] - 1

    sxxp_return = df.loc['SXXR Index', 'return']
    spx_return = df.loc['SPTR500N Index', 'return']

    # remove sxxp and spx
    df = df.drop(['SXXR Index', 'SPTR500N Index'])

    # add region
    df['region'] = df.index.to_series().apply(assign_region)

    # get avg return by region
    df_region = df.groupby('region').mean()

    amer_return = df_region.loc['AMER', 'return']
    amer_rel_perf = amer_return - spx_return
    emea_return = df_region.loc['EMEA', 'return']
    emea_rel_perf = emea_return - sxxp_return

    total_perf = amer_rel_perf / 3 + 2*emea_rel_perf / 3
    my_result_list = [spx_return, sxxp_return, amer_return, emea_return, amer_rel_perf, emea_rel_perf, total_perf]

    return my_result_list


if __name__ == '__main__':

    my_sql = """SELECT entry_date FROM product_market_data where product_id=285 order by entry_date"""
    df_date = pd.read_sql(my_sql, con=engine)

    # df_date = df_date[df_date['entry_date'] > date(2025, 1, 1)]
    date_list = df_date['entry_date'].tolist()

    # create df with date and my_result_list = [amer_return, emea_return, amer_rel_perf, emea_rel_perf, total_perf]
    df = pd.DataFrame(columns=['start_date', 'next_date', 'spx_return', 'sxxp_return', 'amer_return', 'emea_return', 'amer_rel_perf',
                               'emea_rel_perf', 'total_perf'])

    for i in range(len(date_list) - 1):  # len(date_list) - 1 to avoid the last element
        my_date = date_list[i]
        next_date = date_list[i + 1]
        perf_list = get_univ_rel_perf(my_date, next_date)
        df = df._append({'start_date': my_date, 'next_date': next_date, 'spx_return': perf_list[0], 'sxxp_return': perf_list[1],
                         'amer_return': perf_list[2], 'emea_return': perf_list[3], 'amer_rel_perf': perf_list[4],
                         'emea_rel_perf': perf_list[5], 'total_perf': perf_list[6]}, ignore_index=True)
        my_date = find_next_date(my_date)
        print(my_date)

    # add cumul_perf that is the sum of total_perf, not the product
    df['cumul_perf'] = df['total_perf'].cumsum()

    # export to excel in Excel folder
    df.to_excel('Excel/historic_alpha_universe.xlsx', index=False)
