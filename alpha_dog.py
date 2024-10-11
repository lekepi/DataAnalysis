from datetime import date, timedelta
from utils import last_alpha_date, get_weekly_dates
import pandas as pd
from models import engine
import numpy as np


def get_perf_dog(start_date, end_date, region, dog_number, reduction_perc, rolling_alpha_period, rebalancing_frequency,
                  reset_period):

    my_sql = "SELECT id as product_id,ticker,prod_type FROM product;"
    df_product = pd.read_sql(my_sql, con=engine)

    week_number = int(rebalancing_frequency[:-1])
    rebalancing_dates = get_weekly_dates(start_date, end_date, week_number)

    rebalancing_dates_sql = "'" + "','".join([str(x) for x in rebalancing_dates]) + "'"
    my_sql = f"""SELECT entry_date,product_id,mkt_value_usd as notional FROM position WHERE entry_date in 
    ({rebalancing_dates_sql}) and parent_fund_id=1;"""
    df_pos = pd.read_sql(my_sql, con=engine)
    df_pos = df_pos.merge(df_product, on='product_id', how='left')
    df_pos = df_pos[df_pos['prod_type'] == 'Cash']

    if region == 'AMER':
        # keep only when ticker ends with ' US' or ' CN'
        df_pos = df_pos[df_pos['ticker'].str.endswith(' US') | df_pos['ticker'].str.endswith(' CN')]
    elif region == 'EMEA':
        # keep only when ticker does not end with ' US' or ' CN'
        df_pos = df_pos[~df_pos['ticker'].str.endswith(' US') & ~df_pos['ticker'].str.endswith(' CN')]

    filtered_df = df_pos[df_pos['notional'] > 0]
    df_long = filtered_df.groupby('entry_date')['notional'].sum().reset_index()
    # rename column to 'Long'
    df_long.rename(columns={'notional': 'long'}, inplace=True)

    df_pos = df_pos.merge(df_long, on='entry_date', how='left', suffixes=('', '_sum'))

    reduction_rows = []

    for my_date in rebalancing_dates:
        df_pos_date = df_pos[(df_pos['entry_date'] == my_date) & (df_pos['notional'] > 0)]
        # get product_id list for that date

        product_id_list = df_pos_date['product_id'].tolist()
        product_id_list_sql = ','.join([str(x) for x in product_id_list])

        my_sql = f"""SELECT entry_date,product_id,alpha_{rolling_alpha_period} as rolling_alpha FROM product_alpha_rolling 
            WHERE entry_date='{my_date}' and product_id in ({product_id_list_sql}) order by rolling_alpha asc LIMIT {dog_number};"""

        df_alpha = pd.read_sql(my_sql, con=engine)

        for index, row in df_alpha.iterrows():
            product_id = row['product_id']
            notional = df_pos_date[df_pos_date['product_id'] == product_id]['notional'].values[0]
            reduction = notional * reduction_perc / 100
            new_row = {'entry_date': my_date, 'end_date': None, 'product_id': product_id, 'rolling_alpha': row['rolling_alpha'],
                       'reduction': reduction, 'stock_perf': 0, 'index_perf': 0, 'over_perf': 0, 'over_perf_usd': 0}
            reduction_rows.append(new_row)
        # print(f"Rebalancing date: {my_date} - {len(df_alpha)} stocks selected")

    df_reduction = pd.concat([pd.DataFrame(reduction_rows)], ignore_index=True)
    print("df_reduction created")

    #  get list of all product_id
    product_id_list = df_reduction['product_id'].unique()
    # add SXXR Index (845) SPTR500N Index (916)
    product_id_list = np.append(product_id_list, [845, 916])

    product_id_list_sql = ','.join([str(x) for x in product_id_list])

    my_sql = f"""SELECT entry_date,product_id,adj_price FROM product_market_data WHERE entry_date>='{start_date}' 
    and product_id in ({product_id_list_sql});"""
    df_price = pd.read_sql(my_sql, con=engine)

    print("df_price loaded")

    if reset_period != 'None':
        if reset_period == '3Y':
            reset_days = 1460
        elif reset_period == '2Y':
            reset_days = 730
        elif reset_period == '1Y':
            reset_days = 365
        elif reset_period == '6M':
            reset_days = 182

    # join df_product to df_reduction
    if not df_reduction.empty:
        df_reduction = df_reduction.merge(df_product, on='product_id', how='left')
    print("df_reduction loaded")

    # get all dates from df_reduction
    all_dates = df_reduction['entry_date'].unique()

    result = []

    my_last_alpha_date = last_alpha_date()

    for my_date in all_dates:
        my_end_date = my_date + timedelta(days=reset_days)
        if my_end_date > my_last_alpha_date:
            my_end_date = my_last_alpha_date

        df_reduction_date = df_reduction[df_reduction['entry_date'] == my_date]

        # get from df_price
        sxxr_start = df_price[(df_price['product_id'] == 845) & (df_price['entry_date'] == my_date)]['adj_price'].values[0]
        sxxr_end = df_price[(df_price['product_id'] == 845) & (df_price['entry_date'] == my_end_date)]['adj_price'].values[0]
        sptr_start = df_price[(df_price['product_id'] == 916) & (df_price['entry_date'] == my_date)]['adj_price'].values[0]
        sptr_end = df_price[(df_price['product_id'] == 916) & (df_price['entry_date'] == my_end_date)]['adj_price'].values[0]
        total_usd = 0

        for index, row in df_reduction_date.iterrows():
            ticker = row['ticker']
            product_id = row['product_id']
            reduction = row['reduction']
            start_price = df_price[(df_price['product_id'] == product_id) & (df_price['entry_date'] == my_date)]['adj_price'].values[0]

            try:
                end_price = df_price[(df_price['product_id'] == product_id) & (df_price['entry_date'] == my_end_date)]['adj_price'].values[0]
            except:
                print(f"Error: {my_date} - {product_id} - {ticker} - {my_end_date}")
                end_price = start_price

            stock_perf = (end_price - start_price) / start_price

            if ticker[-3:] == ' US' or ticker[-3:] == ' CN':
                index_perf = (sptr_end - sptr_start) / sptr_start
            else:
                index_perf = (sxxr_end - sxxr_start) / sxxr_start

            over_perf = stock_perf - index_perf
            over_perf_usd = reduction * over_perf
            # update df_reduction
            df_reduction.loc[(df_reduction['entry_date'] == my_date) & (df_reduction['product_id'] == product_id), 'stock_perf'] = stock_perf
            df_reduction.loc[(df_reduction['entry_date'] == my_date) & (df_reduction['product_id'] == product_id), 'index_perf'] = index_perf
            df_reduction.loc[(df_reduction['entry_date'] == my_date) & (df_reduction['product_id'] == product_id), 'over_perf'] = over_perf
            df_reduction.loc[(df_reduction['entry_date'] == my_date) & (df_reduction['product_id'] == product_id), 'over_perf_usd'] = over_perf_usd

            total_usd = total_usd + over_perf_usd

        # add alpha_diff to df_reduction
        result.append({'entry_date': my_date, 'total_usd': -total_usd})

        print(f"{my_date}: {total_usd}")
    # convert result into dataframe
    df_result = pd.DataFrame(result)

    excel_name = f"{start_date}_{region}_{dog_number}_{reduction_perc}_{rolling_alpha_period}_{rebalancing_frequency}_{reset_period}.xlsx"
    # save in excel folder
    df_result.to_excel(f"excel/perf_dog_{excel_name}", index=False)
    df_reduction.to_excel(f"excel/reduction_perf_{excel_name}", index=False)


def get_alpha_dog(start_date, end_date, region, dog_number, reduction_perc, rolling_alpha_period, rebalancing_frequency,
                  reset_period, reduction_method):
    my_sql = f"""SELECT entry_date,product_id,mkt_value_usd as notional,alpha_usd as alpha FROM position WHERE entry_date>='{start_date}' and 
        entry_date<='{end_date}' and parent_fund_id=1;"""
    df_pos = pd.read_sql(my_sql, con=engine)
    print("df_pos loaded")

    my_sql = "SELECT id as product_id,ticker,prod_type FROM product;"
    df_product = pd.read_sql(my_sql, con=engine)

    df_pos = df_pos.merge(df_product, on='product_id', how='left')
    # keep only prod_type='Cash'
    df_pos = df_pos[df_pos['prod_type'] == 'Cash']

    if region == 'AMER':
        # keep only when ticker ends with ' US' or ' CN'
        df_pos = df_pos[df_pos['ticker'].str.endswith(' US') | df_pos['ticker'].str.endswith(' CN')]
    elif region == 'EMEA':
        # keep only when ticker does not end with ' US' or ' CN'
        df_pos = df_pos[~df_pos['ticker'].str.endswith(' US') & ~df_pos['ticker'].str.endswith(' CN')]

    filtered_df = df_pos[df_pos['notional'] > 0]
    df_long = filtered_df.groupby('entry_date')['notional'].sum().reset_index()
    # rename column to 'Long'
    df_long.rename(columns={'notional': 'long'}, inplace=True)

    df_pos = df_pos.merge(df_long, on='entry_date', how='left', suffixes=('', '_sum'))
    df_pos['alpha_perc'] = df_pos['alpha'] / df_pos['long'] * 100

    week_number = int(rebalancing_frequency[:-1])
    rebalancing_dates = get_weekly_dates(start_date, end_date, week_number)

    reduction_rows = []

    for my_date in rebalancing_dates:
        df_pos_date = df_pos[(df_pos['entry_date'] == my_date) & (df_pos['notional'] > 0)]
        # get product_id list for that date

        product_id_list = df_pos_date['product_id'].tolist()
        product_id_list_sql = ','.join([str(x) for x in product_id_list])

        my_sql = f"""SELECT entry_date,product_id,alpha_{rolling_alpha_period} as rolling_alpha FROM product_alpha_rolling 
        WHERE entry_date='{my_date}' and product_id in ({product_id_list_sql}) order by rolling_alpha asc LIMIT {dog_number};"""

        df_alpha = pd.read_sql(my_sql, con=engine)

        for index, row in df_alpha.iterrows():
            product_id = row['product_id']
            notional = df_pos_date[df_pos_date['product_id'] == product_id]['notional'].values[0]
            reduction = notional * reduction_perc/100
            new_row = {'entry_date': my_date, 'product_id': product_id, 'rolling_alpha': row['rolling_alpha'],
                        'reduction': reduction}
            reduction_rows.append(new_row)
        # print(f"Rebalancing date: {my_date} - {len(df_alpha)} stocks selected")

    df_reduction = pd.concat([pd.DataFrame(reduction_rows)], ignore_index=True)
    print("df_reduction created")

    #  get list of all product_id
    product_id_list = df_reduction['product_id'].unique()
    product_id_list_sql = ','.join([str(x) for x in product_id_list])

    my_sql = f"""SELECT entry_date,product_id,adj_price FROM product_market_data WHERE entry_date>='{start_date}' 
    and entry_date<='{end_date}' and product_id in ({product_id_list_sql});"""

    df_price = pd.read_sql(my_sql, con=engine)
    # merge df_price to df_pos
    df_pos = df_pos.merge(df_price, on=['product_id', 'entry_date'], how='left')

    print("df_price loaded")

    if reset_period != 'None':
        if reset_period == '3Y':
            reset_days = 1460
        elif reset_period == '2Y':
            reset_days = 730
        elif reset_period == '1Y':
            reset_days = 365
        elif reset_period == '6M':
            reset_days = 182

    # join df_product to df_reduction
    if not df_reduction.empty:
        df_reduction = df_reduction.merge(df_product, on='product_id', how='left')
    print("df_reduction loaded")

    # get all dates from df_reduction
    all_dates = df_reduction['entry_date'].unique()

    result = []

    for my_date in all_dates:
        df_pos['new_notional'] = df_pos['notional']
        df_pos['new_alpha'] = df_pos['alpha']

        df_reduction_date = df_reduction[df_reduction['entry_date'] == my_date]
        for index, row in df_reduction_date.iterrows():
            product_id = row['product_id']
            reduction = row['reduction']
            price = df_pos[(df_pos['product_id'] == product_id) & (df_pos['entry_date'] == my_date)]['adj_price'].values[0]
            stock_qty = reduction / price

            if reset_period != 'None':
                reset_date = my_date + timedelta(days=reset_days)

                condition = (df_pos['product_id'] == product_id) & (df_pos['entry_date'] >= my_date) & \
                            (df_pos['notional'] > 0) & (df_pos['entry_date'] <= reset_date)
            else:
                condition = (df_pos['product_id'] == product_id) & (df_pos['entry_date'] >= my_date) & (df_pos['notional'] > 0)

            if reduction_method == 'stock_number':
                adj_price = df_pos.loc[condition, 'adj_price']
                reduction_value = stock_qty * adj_price
                df_pos.loc[condition, 'new_notional'] = np.maximum(0, df_pos.loc[condition, 'new_notional'] - reduction_value)
                df_pos.loc[condition, 'new_alpha'] = (df_pos.loc[condition, 'alpha'] / df_pos.loc[condition, 'notional'] *
                                                      df_pos.loc[condition, 'new_notional'])
            else:  # fixed $
                df_pos.loc[condition, 'new_notional'] = np.maximum(0, df_pos.loc[condition, 'new_notional'] - reduction)
                df_pos.loc[condition, 'new_alpha'] = (
                            df_pos.loc[condition, 'alpha'] / df_pos.loc[condition, 'notional'] *
                            df_pos.loc[condition, 'new_notional'])

        filtered_df = df_pos[df_pos['notional'] > 0]
        df_new_long = filtered_df.groupby('entry_date')['new_notional'].sum().reset_index()
        # rename column to 'Long'
        df_new_long.rename(columns={'new_notional': 'new_long_sum'}, inplace=True)

        if 'new_long_sum' in df_pos.columns:
            df_pos.drop(columns=['new_long_sum'], inplace=True)

        df_pos = df_pos.merge(df_new_long, on='entry_date', how='left')
        df_pos['new_alpha_perc'] = df_pos['new_alpha'] / df_pos['new_long_sum'] * 100
        alpha_perc_sum = df_pos['alpha_perc'].sum()
        new_alpha_perc_sum = df_pos['new_alpha_perc'].sum()
        alpha_perc_diff = new_alpha_perc_sum - alpha_perc_sum

        alpha_sum = df_pos['alpha'].sum()
        new_alpha_sum = df_pos['new_alpha'].sum()
        alpha_diff = new_alpha_sum - alpha_sum

        # add alpha_diff to df_reduction
        result.append({'entry_date': my_date, 'alpha_perc_sum': alpha_perc_sum, 'new_alpha_perc_sum': new_alpha_perc_sum,
                       'alpha_perc_diff': alpha_perc_diff, 'alpha_sum': alpha_sum, 'new_alpha_sum': new_alpha_sum,
                       'alpha_diff': alpha_diff})

        print(f"{my_date}: {alpha_diff}")
    # convert result into dataframe
    df_result = pd.DataFrame(result)

    excel_name = f"{start_date}_{region}_{dog_number}_{reduction_perc}_{rolling_alpha_period}_{rebalancing_frequency}_{reset_period}_{reduction_method}.xlsx"
    # save in excel folder
    df_result.to_excel(f"excel/alpha_dog_{excel_name}", index=False)
    df_reduction.to_excel(f"excel/reduction_{excel_name}", index=False)


if __name__ == '__main__':

    start_date = date(2019, 4, 1)  # date(2024, 4, 1)
    end_date = last_alpha_date()
    region = 'EMEA'  # 'AMER', 'EMEA', 'All'
    dog_number = 10
    reduction_perc = 10
    rolling_alpha_period = '6M'  # '3M', '6M', '1Y'
    rebalancing_frequency = '2W'  # '2W', '4W', '8W', '12W'
    reset_period = '1Y'  # '6M', '1Y', '2Y', '3Y'
    reduction_method = 'stock_number'  # 'stock_number', 'static_usd'

    get_perf_dog(start_date, end_date, region, dog_number, reduction_perc, rolling_alpha_period,
                 rebalancing_frequency, reset_period)

    get_alpha_dog(start_date, end_date, region, dog_number, reduction_perc, rolling_alpha_period,
                  rebalancing_frequency, reset_period, reduction_method)
    exit()

    rolling_alpha_period_list = ['3M', '6M', '1Y']
    reset_period_list = ['6M', '1Y', '2Y']

    for rolling_alpha_period in rolling_alpha_period_list:
        for reset_period in reset_period_list:
            get_alpha_dog(start_date, end_date, region, dog_number, reduction_perc, rolling_alpha_period,
                          rebalancing_frequency, reset_period, reduction_method)
