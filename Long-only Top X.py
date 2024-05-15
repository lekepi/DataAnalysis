import pandas as pd
from models import engine
from utils import last_alpha_date
from datetime import date

def get_top_position(position_count, fee_rate):

    end_date = last_alpha_date()

    # top_position
    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd,alpha_usd,pnl_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE T1.parent_fund_id=1 and entry_date>='2019-04-01' and entry_date<='{end_date}' and quantity>0  and prod_type='Cash' order by entry_date,mkt_value_usd desc;"""

    df_top_pos = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as notional_usd FROM position T1
    JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
    and T1.parent_fund_id=1 and entry_date>='2019-04-01' and entry_date<='{end_date}' group by T1.entry_date
    Order by T1.entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df['top_usd'] = 0
    df['alpha_usd'] = 0
    df['pnl_usd'] = 0

    for index, row in df.iterrows():
        df.loc[index, 'top_usd'] += df_top_pos.loc[index][:position_count]['mkt_value_usd'].sum()
        df.loc[index, 'alpha_usd'] += df_top_pos.loc[index][:position_count]['alpha_usd'].sum()
        df.loc[index, 'pnl_usd'] += df_top_pos.loc[index][:position_count]['pnl_usd'].sum()

    df['pnl_bp'] = df['pnl_usd'] / df['top_usd']
    df['pnl_bp_fee'] = df['pnl_bp'] - fee_rate/100/252
    # calculate perf with sum product
    df['pnl_bp_sum'] = (1 + df['pnl_bp']).cumprod() - 1
    df['pnl_bp_sum_fee'] = (1 + df['pnl_bp_fee']).cumprod() - 1

    total_perf = df['pnl_bp_sum_fee'][-1]
    duration_day = (df.index[-1] - df.index[0]).days
    annualized_perf = (1 + total_perf) ** (365.25 / duration_day) - 1

    df['month_year'] = df.index.to_period('M')
    # get list of month_year
    month_year_list = df['month_year'].unique()

    df.to_excel(rf'Excel\top_position {position_count}.xlsx', index=True, sheet_name='Top Position')

    # create empty df with index  = month_year_list
    df_monthly_perf = pd.DataFrame(index=month_year_list, columns=['pnl_bp'])
    # df_monthly_perf['pnl_bp'] = product of (1 + pnl_bp_fee) for each month
    for month_year in month_year_list:
        df_monthly_perf.loc[month_year, 'pnl_bp'] = (1 + df[df['month_year'] == month_year]['pnl_bp_fee']).prod() - 1

    with pd.ExcelWriter(rf'Excel\top_position {position_count}.xlsx', mode='a') as writer:
        df_monthly_perf.to_excel(writer, sheet_name='Monthly Perf', index=True)

    # create summary sheet in excel
    with pd.ExcelWriter(rf'Excel\top_position {position_count}.xlsx', mode='a') as writer:
        df_summary = pd.DataFrame(data={'Total Perf': [total_perf], 'Annualized Perf': [annualized_perf]})
        df_summary.to_excel(writer, sheet_name='Summary', index=False)

    # get distinct year from df_top_pos
    year_list = df_top_pos.index.year.unique()
    # get last date for each year

    date_list = [df.index[0]]

    for year in year_list:
        if year != 2019 and (year < date.today().year or date.today().month > 6):
            jun_date = df.index[(df.index.year == year) & (df.index.month == 6)].max()
            date_list.append(jun_date)
        if year != date.today().year:
            last_date = df.index[df.index.year == year].max()
            date_list.append(last_date)

    my_dict = {}

    for my_date in date_list:
        # get df_top_pos for index = my_date top 20 rows
        df_top_pos_date = df_top_pos[df_top_pos.index == my_date][:position_count]
        # keep ticker only
        df_top_pos_date = df_top_pos_date['ticker']
        # sort alphabetically
        df_top_pos_date = df_top_pos_date.sort_values()
        my_date_str = my_date.strftime('%Y-%m-%d')
        # get list
        df_top_pos_date = df_top_pos_date.tolist()
        # put it in df_name_turnover with index = with column name = my_date
        my_dict[my_date_str] = df_top_pos_date

    # turn into df
    df_name_turnover = pd.DataFrame(my_dict)
    # write to excel
    with pd.ExcelWriter(rf'Excel\top_position {position_count}.xlsx', mode='a') as writer:
        df_name_turnover.to_excel(writer, sheet_name='Name Turnover', index=True)

    # remove column that contains '-06-'
    df_name_year = df_name_turnover.loc[:, ~df_name_turnover.columns.str.contains('-06-')]

    dict_to_perc = {}

    for col in df_name_year.columns:
        # if first col
        if col != df_name_year.columns[0]:
            new_list = df_name_year[col].tolist()
            # get diff
            diff_list = list(set(new_list) - set(old_list))
            dict_to_perc[col] = len(diff_list) / len(old_list)
        old_list = df_name_year[col].tolist()

    df_name_turnover_perc = pd.DataFrame(dict_to_perc, index=[0])
    with pd.ExcelWriter(rf'Excel\top_position {position_count}.xlsx', mode='a') as writer:
        df_name_turnover_perc.to_excel(writer, sheet_name='Name Turnover Perc', index=False)


if __name__ == '__main__':

    stock_number = 20
    fee_perc = 0  #0.3
    get_top_position(stock_number, fee_perc)
