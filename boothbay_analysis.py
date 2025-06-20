
from models import engine
import pandas as pd
import numpy as np
from datetime import date


def get_turn_over():
    my_sql = f"""SELECT entry_date,sum(abs(mkt_value_usd)) as gross_usd from position 
    WHERE parent_fund_id=4 and entry_date<'2020-10-31' GROUP BY entry_date order by entry_date;"""
    df_gross = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = f"""SELECT trade_date as entry_date,sum(abs(notional_usd)) as trade_usd FROM trade WHERE parent_fund_id=4 
    and trade_date<'2020-10-31' group by trade_date;"""
    df_trade = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df = pd.merge(df_gross, df_trade, how='left', on='entry_date')
    df = df.fillna(0)

    df['turn_over'] = df['trade_usd'] / df['gross_usd']
    df['year'] = df['entry_date'].dt.year

    # export into excel in excel folder name boothbay_turnover
    df.to_excel(f'excel/boothbay_turnover.xlsx', index=False)


def run_analysis():
    my_sql = f"""SELECT entry_date,T2.ticker,T2.industry_sector_id,T2.industry_group_gics_id,mkt_value_usd,abs(mkt_value_usd) as gross_usd,
        pnl_usd,alpha_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE parent_fund_id=4 
        and entry_date<'2020-10-31'"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df['long_usd'] = np.where(df['mkt_value_usd'] >= 0, df['mkt_value_usd'], 0)
    df['short_usd'] = np.where(df['mkt_value_usd'] < 0, df['mkt_value_usd'], 0)

    df['long_count'] = np.where(df['mkt_value_usd'] >= 0, 1, 0)
    df['short_count'] = np.where(df['mkt_value_usd'] < 0, 1, 0)

    df['long_index_usd'] = np.where(df['industry_sector_id'] == 8, df['long_usd'], 0)
    df['short_index_usd'] = np.where(df['industry_sector_id'] == 8, df['short_usd'], 0)

    # index_usd is the sum of gross_usd when industry_sector_id is 8
    df['index_usd'] = np.where(df['industry_sector_id'] == 8, df['gross_usd'], 0)

    # group by date and side, sum mkt_value_usd, pnl_usd, alpha_usd
    df_grouped = df.groupby(['entry_date']).agg({'long_usd': 'sum', 'short_usd': 'sum', 'pnl_usd': 'sum',
                                                 'alpha_usd': 'sum', 'long_count': 'sum', 'short_count': 'sum',
                                                 'long_index_usd': 'sum', 'short_index_usd': 'sum',
                                                 'index_usd': 'sum'}).reset_index()

    df_grouped['gross_usd'] = df_grouped['long_usd'] - df_grouped['short_usd']
    df_grouped['net_usd'] = df_grouped['long_usd'] + df_grouped['short_usd']
    df_grouped['index_perc'] = df_grouped['index_usd'] / df_grouped['gross_usd'] * 100
    df_grouped['alpha_bp'] = df_grouped['alpha_usd'] / df_grouped['gross_usd'] * 10000
    df_grouped['alpha_bp_cum'] = df_grouped['alpha_bp'].cumsum()

    df_grouped['perf_bp'] = df_grouped['pnl_usd'] / df_grouped['gross_usd'] * 10000
    df_grouped['perf_bp_cum'] = df_grouped['perf_bp'].cumsum()

    # export to excel
    df_grouped.to_excel('Excel/boothbay_analysis.xlsx', index=False)


def get_average_position_size(start_date, end_date):

    my_sql = f"""SELECT avg(abs(mkt_value_usd)) FROM position WHERE parent_fund_id=4 and 
    entry_date>='{start_date}' and entry_date<='{end_date}' and mkt_value_usd>0 and product_id not in (91, 93);"""

    df_long = pd.read_sql(my_sql, con=engine)
    average_position_size_long = df_long.iloc[0, 0]

    my_sql = f"""SELECT avg(abs(mkt_value_usd)) FROM position WHERE parent_fund_id=4 and
    entry_date>='{start_date}' and entry_date<='{end_date}' and mkt_value_usd<0 and product_id not in (91, 93);"""

    df_short = pd.read_sql(my_sql, con=engine)
    average_position_size_short = df_short.iloc[0, 0]

    return average_position_size_long, average_position_size_short


def get_stats_long_short(start_date, end_date):

    my_sql = f"""SELECT mkt_value_usd,abs(mkt_value_usd) as gross_usd,entry_date, product_id FROM position WHERE parent_fund_id=4 and 
    entry_date>='{start_date}' and entry_date<='{end_date}'
    order by entry_date,mkt_value_usd desc;"""

    df_all = pd.read_sql(my_sql, con=engine)

    total_per_day = df_all.groupby('entry_date')['gross_usd'].sum().reset_index()

    long_per_day = df_all[df_all['mkt_value_usd'] > 0].groupby('entry_date')['mkt_value_usd'].sum().reset_index()
    long_per_day.rename(columns={'mkt_value_usd': 'long_mkt_value_usd'}, inplace=True)
    long_count_per_day = df_all[df_all['mkt_value_usd'] > 0].groupby('entry_date')['mkt_value_usd'].count().reset_index()
    long_count_per_day.rename(columns={'mkt_value_usd': 'long_count'}, inplace=True)

    short_per_day = df_all[df_all['mkt_value_usd'] < 0].groupby('entry_date')['mkt_value_usd'].sum().reset_index()
    short_per_day.rename(columns={'mkt_value_usd': 'short_mkt_value_usd'}, inplace=True)
    short_count_per_day = df_all[df_all['mkt_value_usd'] < 0].groupby('entry_date')['mkt_value_usd'].count().reset_index()
    short_count_per_day.rename(columns={'mkt_value_usd': 'short_count'}, inplace=True)

    df_result = pd.merge(total_per_day, long_per_day, on='entry_date', how='left')
    df_result = pd.merge(df_result, short_per_day, on='entry_date', how='left')
    df_result = pd.merge(df_result, long_count_per_day, on='entry_date', how='left')
    df_result = pd.merge(df_result, short_count_per_day, on='entry_date', how='left')

    df_filter = df_all[~df_all['product_id'].isin([91, 93])]

    df_top5 = df_filter.groupby('entry_date').head(5)  # Get top 5 mkt_value_usd per day
    df_top5_sum = df_top5.groupby('entry_date')['mkt_value_usd'].sum().reset_index()
    df_top5_sum.rename(columns={'mkt_value_usd': 'top5_mkt_value_usd'}, inplace=True)
    df_result = pd.merge(df_result, df_top5_sum, on='entry_date', how='left')

    df_bottom5 = df_filter.groupby('entry_date').tail(5)  # Get bottom 5 mkt_value_usd per day
    df_bottom5_sum = df_bottom5.groupby('entry_date')['mkt_value_usd'].sum().reset_index()
    df_bottom5_sum.rename(columns={'mkt_value_usd': 'bottom5_mkt_value_usd'}, inplace=True)
    df_result = pd.merge(df_result, df_bottom5_sum, on='entry_date', how='left')

    df_result['top5_long_perc'] = df_result['top5_mkt_value_usd'] / df_result['gross_usd'] * 100
    df_result['bottom5_long_perc'] = df_bottom5_sum['bottom5_mkt_value_usd'] / df_result['gross_usd'] * 100

    df_result['avg_long_perc'] = df_result['long_mkt_value_usd'] / df_result['long_count'] / df_result['gross_usd'] * 100
    df_result['avg_short_perc'] = df_result['short_mkt_value_usd'] / df_result['short_count'] / df_result['gross_usd'] * 100

    # get the average percentage
    avg_top5_long = df_result['top5_long_perc'].mean()
    avg_bottom5_long = df_result['bottom5_long_perc'].mean()

    avg_long = df_result['avg_long_perc'].mean()
    avg_short = df_result['avg_short_perc'].mean()

    return avg_long, avg_short, avg_top5_long, avg_bottom5_long


if __name__ == '__main__':

    get_turn_over()

    start_date = date(2019, 4, 1)
    end_date = date(2020, 10, 31)

    avg_long, avg_short, avg_top5_long, avg_bottom5_long = get_stats_long_short(start_date, end_date)

    pass