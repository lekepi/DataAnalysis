import sys

import pandas as pd
from models import engine, LiquidityStress, session
from sqlalchemy import func
from datetime import date, timedelta
import numpy as np
import openpyxl


def last_weekday_of_year(year):
    last_day = date(year, 12, 31)  # Set initial date to the last day of the given year
    while last_day.weekday() >= 5:  # 5 and 6 represent Saturday and Sunday
        last_day -= timedelta(days=1)
    return last_day


def increase_column_width(file_path):
    wb = openpyxl.load_workbook(file_path)
    for sheet in wb.sheetnames:
        ws = wb[sheet]

        # Loop through all columns in the sheet
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter  # Get the column letter

            # Determine the maximum length of the data in the column
            for cell in column:
                try:
                    max_length = max(max_length, len(str(cell.value)))
                except:
                    pass

            # Increase the width of the column based on the maximum length found
            adjusted_width = max_length + 2  # Adjust by adding some padding
            ws.column_dimensions[column_letter].width = adjusted_width

    # Save the workbook with the new column widths
    wb.save(file_path)


def get_top_long_short_perc():  # as a % of the long: 4.1.1
    my_sql = "SELECT entry_date,long_usd,long_usd-short_usd as gross_usd FROM alpha_summary WHERE parent_fund_id=1 order by entry_date;"
    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = "SELECT entry_date,amount*1000000 as nav_usd FROM aum WHERE type='leveraged' and fund_id=4 and entry_date>='2019-04-01';"
    df_nav = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE prod_type='Cash' and parent_fund_id=1
    and entry_date>='2019-04-01' and entry_date<'{date.today()}' and mkt_value_usd is Not NULL order by entry_date;"""
    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df_position = pd.merge(df_position, df_long, on='entry_date', how='left')
    df_position = pd.merge(df_position, df_nav, on='entry_date', how='left')
    df_position['nav_usd'] = df_position['nav_usd'].fillna(method='ffill')

    df_position['pos % of Long'] = df_position['mkt_value_usd'] / df_position['long_usd']
    df_position['pos % of Gross'] = df_position['mkt_value_usd'] / df_position['gross_usd']
    df_position['pos % of NAV'] = df_position['mkt_value_usd'] / df_position['nav_usd']

    df_position_long = df_position[df_position['mkt_value_usd'] > 0]
    df_position_short = df_position[df_position['mkt_value_usd'] < 0]

    count_list = [20, 30, 40]
    df_avg_size = pd.DataFrame(columns=['count', 'Avg Long Pos vs Long', 'Avg Short Pos vs Long',
                                                   'Avg Long Pos vs NAV - Low Vol', 'Avg Short Pos vs NAV - Low Vol',
                                                    'Avg Long Pos vs NAV - Alto', 'Avg Short Pos vs NAV - Alto'])

    for count in count_list:
        df_long_avg = df_position_long.sort_values(by=['entry_date', 'pos % of Long'], ascending=[True, False])
        result = df_long_avg.groupby('entry_date').head(count).groupby('entry_date')[
            'pos % of Long'].mean().reset_index()
        avg_long_position = round(result['pos % of Long'].mean() * 100, 2)

        df_gross_avg = df_position_long.sort_values(by=['entry_date', 'pos % of Gross'], ascending=[True, False])
        result = df_gross_avg.groupby('entry_date').head(count).groupby('entry_date')[
            'pos % of Gross'].mean().reset_index()
        avg_long_position_gross = round(result['pos % of Gross'].mean() * 100, 2)


        df_nav_avg = df_position_long.sort_values(by=['entry_date', 'pos % of NAV'], ascending=[True, False])
        result = df_nav_avg.groupby('entry_date').head(count).groupby('entry_date')['pos % of NAV'].mean().reset_index()
        avg_long_position_nav = round(result['pos % of NAV'].mean() * 100, 2)

        df_short_avg = df_position_short.sort_values(by=['entry_date', 'pos % of Long'], ascending=[True, True])
        result = df_short_avg.groupby('entry_date').head(count).groupby('entry_date')[
            'pos % of Long'].mean().reset_index()
        avg_short_position_vs_long = round(result['pos % of Long'].mean() * 100, 2)

        df_gross_avg = df_position_short.sort_values(by=['entry_date', 'pos % of Gross'], ascending=[True, True])
        result = df_gross_avg.groupby('entry_date').head(count).groupby('entry_date')[
            'pos % of Gross'].mean().reset_index()
        avg_short_position_gross = round(result['pos % of Gross'].mean() * 100, 2)

        df_nav_avg = df_position_short.sort_values(by=['entry_date', 'pos % of NAV'], ascending=[True, True])
        result = df_nav_avg.groupby('entry_date').head(count).groupby('entry_date')['pos % of NAV'].mean().reset_index()
        avg_short_position_nav = round(result['pos % of NAV'].mean() * 100, 2)

        df_avg_size = df_avg_size._append(pd.DataFrame({'count': count, 'Avg Long Pos vs Long': avg_long_position,
                                                        'Avg Short Pos vs Long': -avg_short_position_vs_long,
                                                        'Avg Long Pos vs Gross': avg_long_position_gross,
                                                        'Avg Short Pos vs Gross': -avg_short_position_gross,
                                                        'Avg Long Pos vs NAV - Low Vol': avg_long_position_nav,
                                                        'Avg Short Pos vs NAV - Low Vol': -avg_short_position_nav,
                                                        'Avg Long Pos vs NAV - Alto': avg_long_position_nav * 2,
                                                        'Avg Short Pos vs NAV - Alto': -avg_short_position_nav * 2
                                                        }, index=[0]), ignore_index=True)

    alto_long_top_30_nav = df_avg_size[df_avg_size['count'] == 30]['Avg Long Pos vs NAV - Alto'].values[0]
    low_vol_long_top_30_nav = df_avg_size[df_avg_size['count'] == 30]['Avg Long Pos vs NAV - Low Vol'].values[0]
    alto_long_top_30_gross = df_avg_size[df_avg_size['count'] == 30]['Avg Long Pos vs Gross'].values[0]
    low_vol_long_top_30_gross = df_avg_size[df_avg_size['count'] == 30]['Avg Long Pos vs Gross'].values[0]

    alto_short_top_30_nav = df_avg_size[df_avg_size['count'] == 30]['Avg Short Pos vs NAV - Alto'].values[0]
    low_vol_short_top_30_nav = df_avg_size[df_avg_size['count'] == 30]['Avg Short Pos vs NAV - Low Vol'].values[0]
    alto_short_top_30_gross = df_avg_size[df_avg_size['count'] == 30]['Avg Short Pos vs Gross'].values[0]
    low_vol_short_top_30_gross = df_avg_size[df_avg_size['count'] == 30]['Avg Short Pos vs Gross'].values[0]

    values = [['Alto Long Top 30', alto_long_top_30_nav, alto_long_top_30_gross],
              ['Low Vol Long Top 30', low_vol_long_top_30_nav, low_vol_long_top_30_gross],
              ['Alto Short Top 30', alto_short_top_30_nav, alto_short_top_30_gross],
              ['Low Vol Short Top 30', low_vol_short_top_30_nav, low_vol_short_top_30_gross]]

    df_position_vs_capital = pd.DataFrame(values, columns=['Name', 'vs NAV', 'vs Gross'])

    df_top_long_short_perc = df_avg_size[['Avg Long Pos vs Long', 'Avg Short Pos vs Long']]
    return df_top_long_short_perc, df_position_vs_capital


def get_market_cap(my_date):
    my_sql = f"""SELECT product_id,mkt_value_usd, market_cap,prod_type,T3.name as sector FROM position T1 
    JOIN product T2 on T1.product_id=T2.id LEFT JOIN industry_group_gics T3 on T2.industry_group_gics_id=T3.id
    WHERE entry_date='{my_date}' and parent_fund_id=1 and (prod_type='Cash' or prod_type='future');"""
    df = pd.read_sql(my_sql, con=engine)

    conditions = [(df['prod_type'] == 'Future'), (df['market_cap'] > 10000000000),
                  (df['market_cap'] > 3000000000) & (df['market_cap'] <= 10000000000),
                  (df['market_cap'] < 3000000000) & (df['market_cap'].notnull())]

    choices = ['4) Index', '1) >USD 10bn', '2) USD 3-10bn', '3) <USD 3bn']

    # Create the new column using np.select
    df['Market Cap'] = np.select(conditions, choices, default=None)

    long_total = df[df['mkt_value_usd'] > 0]['mkt_value_usd'].sum()
    df['long %'] = 0
    df.loc[df['mkt_value_usd'] > 0, 'long %'] = df['mkt_value_usd'] / long_total * 100
    df['short %'] = 0
    df.loc[df['mkt_value_usd'] < 0, 'short %'] = -df['mkt_value_usd'] / long_total * 100

    # market cap
    df_market_cap = df.groupby('Market Cap')[['long %', 'short %']].sum().reset_index()
    df_market_cap = df_market_cap.sort_values(by='Market Cap', ascending=True)

    df_market_cap['long %'] = df_market_cap['long %'].round(2)
    df_market_cap['short %'] = df_market_cap['short %'].round(2)

    # sector
    df['sector'] = np.where(df['prod_type'] == 'Future', 'Index', df['sector'])

    df_sector = df.groupby('sector')[['long %', 'short %']].sum().reset_index()
    df_sector = df_sector.sort_values(by='long %', ascending=False)

    df_sector['long %'] = df_sector['long %'].round(2)
    df_sector['short %'] = df_sector['short %'].round(2)
    df_sector_x_index = df_sector[df_sector['sector'] != 'Index']
    df_sector_index = df_sector[df_sector['sector'] == 'Index']
    df_sector = pd.concat([df_sector_x_index, df_sector_index], ignore_index=True)

    return df_market_cap, df_sector


def get_turnover_capital():
    my_sql = """SELECT trade_date,T2.ticker,ABS(SUM(T1.notional_usd)) AS trade_usd FROM trade T1 
            JOIN product T2 ON T1.product_id = T2.id WHERE T2.still_active=1 and trade_date>='2019-04-01'
            and T1.parent_fund_id=1 GROUP BY trade_date,ticker ORDER BY trade_date,ticker;"""
    df_trade = pd.read_sql(my_sql, con=engine, parse_dates=['trade_date'])
    df_trade = df_trade.groupby('trade_date')['trade_usd'].sum().reset_index()
    df_trade = df_trade.rename(columns={'trade_date': 'entry_date'})

    my_sql = """SELECT entry_date, sum(abs(mkt_value_usd)) as gross_usd FROM position T1 WHERE T1.parent_fund_id=1 GROUP BY entry_date;"""
    df_gross = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # merge df_trade and df_gross
    df = pd.merge(df_trade, df_gross, on='entry_date', how='left')
    df['turnover'] = df['trade_usd'] / df['gross_usd']

    df_year = df.groupby(df['entry_date'].dt.year)['turnover'].sum().reset_index()
    df_year = df_year.rename(columns={'entry_date': 'year'})

    # adjustment for additions/redemptions
    my_sql = """SELECT entry_date, 1000000*amount as aum FROM aum WHERE type='leveraged' and fund_id=4 and entry_date>='2019-04-01';"""
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df_aum['aum_change'] = df_aum['aum'].diff().shift(-1).abs()
    df_aum['turnover_adj'] = -df_aum['aum_change'] / df_aum['aum']
    # group by year
    df_year_aum = df_aum.groupby(df_aum['entry_date'].dt.year)['turnover_adj'].sum().reset_index()
    df_year_aum = df_year_aum.rename(columns={'entry_date': 'year'})
    df_year = pd.merge(df_year, df_year_aum, on='year', how='left')
    df_year['turnover'] = df_year['turnover'] + df_year['turnover_adj']
    df_year = df_year.drop(columns=['turnover_adj'])
    # remove current year
    current_year = date.today().year
    # df_year = df_year[df_year['year'] != current_year]

    # round to 2 decimals
    df_year['turnover'] = df_year['turnover'].apply(lambda x: round(x, 2))
    # sort by year desc
    df_year = df_year.sort_values(by=['year'])

    average_turnover = df_year['turnover'].mean()
    total_row = pd.DataFrame({'year': ['Avg'], 'turnover': [average_turnover]})
    df_year = pd.concat([df_year, total_row], ignore_index=True)

    df_turnover_capital = df_year
    return df_turnover_capital


def get_turnover_name(name_number, direction='Long'):

    first_date = date(2019, 4, 1)
    current_year = date.today().year

    date_list = [first_date]

    for year in range(2019, current_year):
        last_day = last_weekday_of_year(year)
        date_list.append(last_day)

    df_result = pd.DataFrame(columns=['year', 'turnover'])

    df_old = pd.DataFrame()

    if direction == 'Long':
        mkt_value_sql = " and mkt_value_usd>0 "
    else:
        mkt_value_sql = " and mkt_value_usd<0 "

    for index, my_date in enumerate(date_list):
        my_sql = f"""SELECT T2.ticker,abs(mkt_value_usd) as mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
        WHERE parent_fund_id=1 and T2.prod_type='Cash' and entry_date='{my_date}' {mkt_value_sql}
        order by abs(mkt_value_usd) desc;"""
        df_new = pd.read_sql(my_sql, con=engine)

        if not df_old.empty:
            df_merge = pd.merge(df_old, df_new, on='ticker', how='left')
            # keep name_number first rows
            df_merge = df_merge.head(name_number)
            name_number_temp = df_merge['ticker'].count()
            # count the number of row with mkt_value_usd_y not null
            count = df_merge['mkt_value_usd_y'].count()
            turnover = 1 - count / name_number_temp
            df_result = df_result._append({'year': my_date.year, 'turnover': turnover}, ignore_index=True)
            # change turnover to string pct
            # reformat
        df_old = df_new

    df_result['year'] = df_result['year'].astype(int).astype(str)
    df_result = df_result.sort_values(by=['year'])

    average_turnover = df_result['turnover'].mean()
    total_row = pd.DataFrame({'year': ['Avg'], 'turnover': [average_turnover]})
    df_result = pd.concat([df_result, total_row], ignore_index=True)
    df_result['turnover'] = df_result['turnover'].apply(lambda x: round(x * 100, 2))
    df_result = df_result.rename(columns={'turnover': 'Turnover %'})

    df_turnover_name = df_result
    return df_turnover_name


def get_instrument_traded():
    start_date = date(2019, 4, 1)
    end_date = date(date.today().year, 1, 1) - timedelta(days=1)
    my_sql = f"""SELECT T1.trade_date,T2.ticker,T2.prod_type,T4.name as country,T4.continent,
        T5.name as sector,T6.generic_future as security,abs(sum(T1.notional_usd)) as notional_usd 
        FROM trade T1 JOIN product T2 on T1.product_id=T2.id
        LEFT JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
        LEFT JOIN industry_sector T5 on T2.industry_sector_id=T5.id 
        LEFT JOIN security T6 on T2.security_id=T6.id WHERE parent_fund_id=1 and trade_date>='{start_date}'
        and trade_date<='{end_date}' and T2.ticker <>'EDZ2 CME'
        GROUP BY trade_date,T2.ticker,T2.prod_type,country,continent,sector order by T2.ticker,trade_date;"""
    df_trade = pd.read_sql(my_sql, con=engine, parse_dates=['trade_date'])
    df_trade['asset_class'] = 'Other'
    # if prod_type is Cash, and country in ('united States, Canada, Switzerland'), then asset_class is 'Cash'
    df_trade.loc[(df_trade['prod_type'] == 'Cash') &
                 (df_trade['country'].isin(['United States', 'Canada', 'Switzerland'])), 'asset_class'] = 'Cash Equity'
    # if prod_type is cash and country not in ('united States, Canada, Switzerland'), then asset_class is 'CFD'
    df_trade.loc[(df_trade['prod_type'] == 'Cash') &
                 (~df_trade['country'].isin(['United States', 'Canada', 'Switzerland'])), 'asset_class'] = 'CFD'
    # if ticker in ('SXO1 EUX', 'ES1 CME'), then asset_class is 'Equity Index'
    df_trade.loc[df_trade['security'].isin(['SXO1 EUX', 'ES1 CME']), 'asset_class'] = 'Equity Index'
    df_trade['notional_usd'] = df_trade['notional_usd'].fillna(0)
    # pivot table with asset_class as columns, year as index, and sum of notional_usd as values
    df_asset_class = pd.pivot_table(df_trade, values='notional_usd', index=df_trade['trade_date'].dt.year,
                                    columns='asset_class', aggfunc='sum', fill_value=0)

    df_asset_class = df_asset_class.div(df_asset_class.sum(axis=1), axis=0).round(4)
    df_asset_class = df_asset_class * 100
    df_asset_class = df_asset_class.reset_index()
    df_asset_class = df_asset_class.rename(columns={'trade_date': 'Year'})
    df_instrument_traded = df_asset_class
    return df_instrument_traded


def get_stress_test():
    # get max entry_date from LiquidityStress table using sqlalchemy
    my_date = session.query(func.max(LiquidityStress.entry_date)).scalar()
    my_sql = f"""SELECT adv,days,value FROM liquidity_stress WHERE entry_date='{my_date}' and parent_fund_id=1"""
    df = pd.read_sql(my_sql, con=engine)
    # pivot with days as columns, adv as index, and value as values
    df_stress_test = df.pivot(index='adv', columns='days', values='value')
    df_stress_test = df_stress_test.reset_index()

    df_stress_test.columns = [str(col) + ' Days' for col in df_stress_test.columns]
    my_date_str = my_date.strftime('%b %Y').upper()
    # rename first col to my_date_str
    df_stress_test = df_stress_test.rename(columns={'adv Days': my_date_str})

    return df_stress_test


if __name__ == '__main__':

    # get_turnover_name(30, 'Short')
    # sys.exit()

    today = date.today()
    file_path = 'Excel/DDQ Output.xlsx'

    df_top_long_short_perc = None
    df_market_cap = None
    df_msci_sector = None
    df_sector = None
    df_turnover_capital = None
    df_turnover_name = None
    df_position_vs_capital = None
    df_instrument_traded = None
    df_stress_test = None

    df_top_long_short_perc, df_position_vs_capital = get_top_long_short_perc()  # 4.1.1 & 4.1.12
    df_market_cap, df_sector = get_market_cap(today)  # 4.1.8 & 4.1.9
    df_turnover_capital = get_turnover_capital()  # 4.1.10
    df_turnover_name = get_turnover_name(name_number=30)  # 4.1.10
    df_instrument_traded = get_instrument_traded()  # 4.1.14
    df_stress_test = get_stress_test()  # 4.4.2

    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        # Write each dataframe to a different sheet
        if df_top_long_short_perc is not None:
            df_top_long_short_perc.to_excel(writer, sheet_name='4_1_1 Top Long Short %', index=False)

        if df_market_cap is not None:
            df_market_cap.to_excel(writer, sheet_name='4_1_8 Market Cap', index=False)

        if df_sector is not None:
            df_sector.to_excel(writer, sheet_name='4_1_9 Sector', index=False)

        if df_turnover_capital is not None:
            df_turnover_capital.to_excel(writer, sheet_name='4_1_10 Turnover Capital', index=False)

        if df_turnover_name is not None:
            df_turnover_name.to_excel(writer, sheet_name='4_1_10 Turnover Name', index=False)

        if df_position_vs_capital is not None:
            df_position_vs_capital.to_excel(writer, sheet_name='4_1_12 Position vs Capital', index=False)

        if df_instrument_traded is not None:
            df_instrument_traded.to_excel(writer, sheet_name='4_1_14 Instrument Traded', index=False)

        if df_stress_test is not None:
            df_stress_test.to_excel(writer, sheet_name='4_4_2 Stress Test', index=False)

    increase_column_width(file_path)


