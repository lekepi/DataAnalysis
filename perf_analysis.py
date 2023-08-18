import pandas as pd
from models import engine, session
from datetime import date, timedelta


def get_missing_date():

    today = date.today()
    my_sql = f"""Select entry_date,count(id) as position from position WHERE entry_date>= '2019-04-01'
             and entry_date<'{today}' group by entry_date order by entry_date"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = "Select entry_date,count(id) as nav from nav_account_statement group by entry_date order by entry_date"
    df_nav = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df = df.merge(df_nav, on='entry_date', how='outer')

    # keep row with df['nav'] = nan
    df = df[df['nav'].isnull()]

    # remove all first of January:
    df = df[~((df['entry_date'].dt.month == 1) & (df['entry_date'].dt.day == 1))]
    # remove all 24th of December:
    df = df[~((df['entry_date'].dt.month == 12) & (df['entry_date'].dt.day == 24))]
    # remove all 25th of December:
    df = df[~((df['entry_date'].dt.month == 12) & (df['entry_date'].dt.day == 25))]
    pass


def get_daily_perf():
    my_sql = "SELECT entry_date,data_name,data_daily,data_mtd,data_qtd,data_ytd " \
             "FROM nav_account_statement WHERE active=1 and status='MonthEnd' and entry_date>='2019-04-01' " \
             "order by entry_date;"
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # convert entry_date to date:
    df['entry_date'] = df['entry_date'].dt.date
    # reformat entry_date into YearMonth:
    df['entry_date'] = df['entry_date'].apply(lambda x: x.strftime('%Y-%m'))
    df_class = df[df['data_name'].isin(['RETURN USD CLASS F', 'RETURN USD CLASS L'])]
    # pivot df_class:
    pivot_df_class = df_class.pivot(index='entry_date', columns='data_name', values='data_mtd')

    start = date(2019, 3, 1)
    end = date.today()
    date_range = pd.date_range(start, end, freq='BM')
    date_list = ["'" + d.strftime('%Y-%m-%d') + "'" for d in date_range]

    # date_list.insert(0, "'2019-04-01'")
    my_date = date.today()
    day = timedelta(days=1)
    day3 = timedelta(days=3)
    if my_date.weekday() == 0:
        my_date -= day3
    else:
        my_date -= day

    date_list.append("'" + my_date.strftime("%Y-%m-%d") + "'")
    date_sql = ",".join(date_list)

    my_sql = f"""SELECT entry_date,ticker,adj_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id 
                     WHERE ticker in ('SXXR Index', 'SPTR500N Index') and entry_date in ({date_sql})"""

    df_index = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # convert entry_date to date
    df_index['entry_date'] = df_index['entry_date'].dt.date
    df_index['entry_date'] = df_index['entry_date'].apply(lambda x: x.strftime('%Y-%m'))
    pivot_df_index = df_index.pivot(index='entry_date', values='adj_price', columns='ticker')

    pivot_df_index['SXXR Return'] = round(pivot_df_index['SXXR Index'].pct_change() * 100, 2)
    pivot_df_index['SPTR500N Return'] = round(pivot_df_index['SPTR500N Index'].pct_change() * 100, 2)

    # keep return column only:
    pivot_df_index = pivot_df_index[['SXXR Return', 'SPTR500N Return']]
    # add benchmark return column that is 2/3 of SXXR Return  and 1/3 of SPTR500N Return
    pivot_df_index['Benchmark Return'] = ((pivot_df_index['SXXR Return'] * 2 + pivot_df_index['SPTR500N Return']) / 3) / 100
    pivot_df_index = pivot_df_index.iloc[1:]

    # merge both pivot:
    pivot_df = pivot_df_class.merge(pivot_df_index, on='entry_date', how='outer')
    # convert index into column

    # add deployed column
    my_sql = "Select entry_date,deployed from aum where entry_date>='2019-04-01' and type='leveraged' order by entry_date"
    df_deployed = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_deployed['entry_date'] = df_deployed['entry_date'].dt.date
    # add the column YearMonth
    df_deployed['entry_date'] = df_deployed['entry_date'].apply(lambda x: x.strftime('%Y-%m'))
    # entry_date becomes index:
    df_deployed.set_index('entry_date', inplace=True)

    # merge pivot_df and df_deployed on index
    pivot_df = pivot_df.merge(df_deployed, left_index=True, right_index=True, how='outer')

    # add year column
    pivot_df['Year'] = pivot_df.index.str[:4]

    # get the list of all disticnt year
    year_list = pivot_df['Year'].unique().tolist()
    # sort the list descending
    year_list.sort(reverse=True)

    # add total in front of the list
    year_list.insert(0, 'Total')

    pivot_df['Class L Adj Return'] = ((pivot_df['RETURN USD CLASS L'] + 2/12) / pivot_df['deployed'] * 100 - 2/12) / 100
    pivot_df['Class F Adj Return'] = ((pivot_df['RETURN USD CLASS F'] + 1/12) / pivot_df['deployed'] * 100 - 1/12) / 100

    pivot_df['Class L Adj Neg Return'] = pivot_df['Class L Adj Return'].apply(lambda x: x if x < 0 else 0)
    pivot_df['Class F Adj Neg Return'] = pivot_df['Class F Adj Return'].apply(lambda x: x if x < 0 else 0)
    pivot_df['Benchmark Neg Return'] = pivot_df['Benchmark Return'].apply(lambda x: x if x < 0 else 0)

    df_metrics = pd.DataFrame(columns=['Alto L Return', 'BM Return', 'Alto Stdev', 'BM Stdev', 'Alto Sharpe', 'BM Sharpe',
                                    'Alto Sortino', 'BM Sortino', 'BM Correl'])

    for year in year_list:
        if year == 'Total':
            df = pivot_df
        else:
            df = pivot_df[pivot_df['Year'] == year]

        # alto_return is the sum product of Class L adj +1
        alto_return = (df['Class L Adj Return'] + 1).prod() - 1
        bm_return = (df['Benchmark Return'] + 1).prod() - 1

        if year == 'Total':
            year_num = (date.today() - date(2019, 4, 1)).days / 365
            alto_annual_return = (alto_return + 1) ** (1 / year_num) - 1
            bm_annual_return = (bm_return + 1) ** (1 / year_num) - 1
        else:
            alto_annual_return = alto_return
            bm_annual_return = bm_return

        alto_stdev = df['Class L Adj Return'].std() * 12 ** 0.5
        bm_stdev = df['Benchmark Return'].std() * 12 ** 0.5
        alto_sharpe = alto_annual_return / alto_stdev
        bm_sharpe = bm_annual_return / bm_stdev
        alto_sortino = alto_annual_return / (df['Class L Adj Neg Return'].std() * 12 ** 0.5)
        bm_sortino = bm_annual_return / (df['Class L Adj Neg Return'].std() * 12 ** 0.5)
        bm_correl = df['Class L Adj Return'].corr(df['Benchmark Return'])

        # add to df_metrics
        df_metrics.loc[year] = [alto_annual_return, bm_annual_return, alto_stdev, bm_stdev, alto_sharpe, bm_sharpe,
                                alto_sortino, bm_sortino, bm_correl]

    df_metrics = df_metrics.T

    return df_metrics

    # get var 1d 99%, 95%, 90%
    # get bell curve + skewness + kurtosis


def get_eom_perf_old():
    my_sql = "SELECT entry_date,data_name,data_daily,data_mtd,data_qtd,data_ytd " \
             "FROM nav_account_statement WHERE active=1 and status<>'MonthEnd' order by entry_date;"
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # keep only: RETURN USD CLASS L
    df_class_l = df[df['data_name'] == 'RETURN USD CLASS L']
    # keep the last date available per month
    # df_class_l_my = df_class_l.groupby([df_class_l['entry_date'].dt.year, df_class_l['entry_date'].dt.month]).last()

    df_class_l_my = df_class_l.groupby([df_class_l['entry_date'].dt.year, df_class_l['entry_date'].dt.month]).last()
    df_class_l_my['full_last_date'] = \
    df_class_l.groupby([df_class_l['entry_date'].dt.year, df_class_l['entry_date'].dt.month])['entry_date'].transform(
        'max')

    # keep only the columns we need
    df_class_l_my = df_class_l_my[['entry_date', 'data_mtd']]

    pass

if __name__ == '__main__':
    get_daily_perf()
    # get_missing_date()
    # get_eom_perf()

