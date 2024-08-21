import pandas as pd
from models import engine
import numpy as np
from datetime import date, datetime


financing_cost_dict = {
    '2019-04': -0.0005,
    '2022-02': -0.0005,
    '2022-03': -0.0006,
    '2022-04': -0.0007,
    '2022-05': -0.0008,
    '2022-06': -0.0009,
    '2022-07': -0.0010,
    '2022-08': -0.0011,
    '2022-09': -0.0011,
    '2022-10': -0.0012,
    '2022-11': -0.0013,
    '2022-12': -0.0014,
    '2023-01': -0.0016,
    '2023-02': -0.0018,
    '2023-03': -0.0022,
    '2023-04': -0.0023,
    '2023-05': -0.0024,
    '2023-06': -0.0025}


def get_attribution(start_date, last_date, net_perc, amer_perc, universe_net_perc):

    # get nav monthly return for alto
    my_sql = f"""SELECT entry_date,data_mtd/100 as nav_perf_raw FROM nav_account_statement WHERE entry_date>='2019-04-01' 
    and entry_date<'{last_date}' and status='MonthEnd' and active=1 and data_name='RETURN USD CLASS L' order by entry_date;"""
    df_nav = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_nav.index = df_nav.index.strftime('%Y-%m')
    print('df_nav done')

    # add nav MTD perf from daily
    my_sql = f"""SELECT entry_date,data_mtd/100 as nav_perf_raw FROM nav_account_statement WHERE status='daily' 
    and active=1 and data_name='RETURN USD CLASS L' order by entry_date desc limit 1;"""
    df_nav_daily = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    current_month_date = df_nav_daily.index[0]
    df_nav_daily.index = df_nav_daily.index.strftime('%Y-%m')
    nav_daily_date = df_nav_daily.index[0]
    if nav_daily_date not in df_nav.index:
        df_nav = pd.concat([df_nav, df_nav_daily])

    # get first_dates (first date of the month)
    my_sql = f"""SELECT MIN(entry_date) AS first_entry_date FROM position WHERE entry_date>='2019-04-01'
                and entry_date<'{last_date}' GROUP BY YEAR(entry_date), MONTH(entry_date) ORDER BY first_entry_date;"""
    df_first_dates = pd.read_sql(my_sql, con=engine)
    # turn into list
    first_dates = df_first_dates['first_entry_date'].tolist()
    # make sure there is no WE, stop code if any
    for i in range(len(first_dates)):
        if first_dates[i].weekday() == 5 or first_dates[i].weekday() == 6:
            print(f'Weekend detected in first_dates: {first_dates[i].weekday()}')
            return
    first_dates_sql = ', '.join([f"'{dt}'" for dt in first_dates])

    # get last_dates (last date of the month)
    my_sql = f"""SELECT MAX(entry_date) AS last_entry_date FROM position WHERE entry_date>='2019-04-01'
                and entry_date<'{last_date}' GROUP BY YEAR(entry_date), MONTH(entry_date) ORDER BY last_entry_date;"""
    df_last_dates = pd.read_sql(my_sql, con=engine)
    # turn into list
    last_dates = df_last_dates['last_entry_date'].tolist()
    # add '2019-03-29' as first date
    last_dates.insert(0, date(2019, 3, 29))

    if current_month_date not in last_dates:
        last_dates.append(current_month_date)

    # make sure there is no WE, stop code if any
    for i in range(len(last_dates)):
        if last_dates[i].weekday() == 5 or last_dates[i].weekday() == 6:
            print(f'Weekend detected in last_dates: {last_dates[i].weekday()}')
            return
    last_dates_sql = ', '.join([f"'{dt}'" for dt in last_dates])

    # get market data for all last_dates
    my_sql = f"""SELECT entry_date,product_id,adj_price,country_id FROM product_market_data T1 JOIN product T2 
    on T1.product_id=T2.id JOIN exchange T3 on T2.exchange_id=T3.id WHERE entry_date IN ({last_dates_sql})
    ORDER by entry_date;"""
    df_market = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_market.index = df_market.index.strftime('%Y-%m')
    df_market['region'] = 'EMEA'
    df_market.loc[df_market['country_id'].isin([40, 234, 143]), 'region'] = 'AMER'  # US and Canada
    # remove country_id
    df_market.drop(columns='country_id', inplace=True)
    print('df_market done')

    # Get position at the start of the month:
    my_sql = f"""SELECT entry_date,product_id,country_id,mkt_value_usd,prod_type FROM position T1 JOIN product T2 on
     T1.product_id=T2.id JOIN exchange T3 on T2.exchange_id=T3.id WHERE 
     (prod_type in ('Cash') or T1.product_id in(437,439)) and entry_date in ({first_dates_sql}) 
     and parent_fund_id=1 ORDER by entry_date;"""
    df_position = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # entry_date format: ('%Y-%m')
    df_position['entry_date'] = df_position['entry_date'].dt.strftime('%Y-%m')

    # if country_id in (40=Canada, 234=US, 143=Mexico) df[region] = 'AMER'
    df_position['region'] = 'EMEA'
    df_position.loc[df_position['country_id'].isin([40, 234, 143]), 'region'] = 'AMER'

    df_position['side'] = 'Long'
    df_position.loc[df_position['mkt_value_usd'] < 0, 'side'] = 'Short'

    df_position['Long AMER Cash'] = df_position.apply(lambda row: row['mkt_value_usd'] if row['mkt_value_usd'] > 0
                                                      and row['prod_type'] == 'Cash' and row['region'] == 'AMER' else 0,
                                                      axis=1)
    df_position['Short AMER Cash'] = df_position.apply(lambda row: row['mkt_value_usd'] if row['mkt_value_usd'] < 0
                                                        and row['prod_type'] == 'Cash' and row['region'] == 'AMER' else 0,
                                                        axis=1)
    df_position['Long EMEA Cash'] = df_position.apply(lambda row: row['mkt_value_usd'] if row['mkt_value_usd'] > 0
                                                      and row['prod_type'] == 'Cash' and row['region'] == 'EMEA' else 0,
                                                      axis=1)
    df_position['Short EMEA Cash'] = df_position.apply(lambda row: row['mkt_value_usd'] if row['mkt_value_usd'] < 0
                                                       and row['prod_type'] == 'Cash' and row['region'] == 'EMEA' else 0,
                                                       axis=1)

    df_position['Short AMER Future'] = df_position.apply(lambda row: row['mkt_value_usd'] if row['mkt_value_usd'] < 0
                                                        and row['prod_type'] == 'Future' and row['region'] == 'AMER' else 0,
                                                        axis=1)
    df_position['Short EMEA Future'] = df_position.apply(lambda row: row['mkt_value_usd'] if row['mkt_value_usd'] < 0
                                                        and row['prod_type'] == 'Future' and row['region'] == 'EMEA' else 0,
                                                        axis=1)
    df_exposure = df_position[['entry_date', 'Long AMER Cash', 'Short AMER Cash', 'Long EMEA Cash', 'Short EMEA Cash',
                               'Short AMER Future', 'Short EMEA Future']]

    df_exposure = df_exposure.groupby(['entry_date']).sum()
    df_exposure['Long'] = df_exposure['Long AMER Cash'] + df_exposure['Long EMEA Cash']
    df_exposure['Short'] = df_exposure['Short AMER Cash'] + df_exposure['Short EMEA Cash'] + df_exposure['Short AMER Future'] + df_exposure['Short EMEA Future']
    df_exposure['Net'] = df_exposure['Long'] + df_exposure['Short']

    print('df_position done')

    # get aum (equivalent for leveraged fund /2)
    my_sql = f"""SELECT entry_date,amount*1000000/2*deployed/100 as aum,deployed FROM aum WHERE fund_id=4 and type='leveraged' and entry_date>='2019-04-01' 
    and entry_date<'{last_date}' order by entry_date;"""
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_aum.index = df_aum.index.strftime('%Y-%m')
    print('df_aum done')

    # merge df_aum, df_long_short, df_nav
    df_exposure = df_aum.join(df_exposure, how='outer')

    df_exposure = df_exposure.join(df_nav, how='outer')
    df_exposure['Nav Perf'] = (df_exposure['nav_perf_raw'] + 0.02/12) / df_exposure['deployed'] * 100 - 0.02/12
    print('df_result done')

    # get universe historically
    my_sql = """SELECT product_id,start_date,end_date,country_id FROM product_universe T1 JOIN product T2
    on T1.product_id=T2.id JOIN exchange T3 on T2.exchange_id=T3.id;"""
    df_product_universe = pd.read_sql(my_sql, con=engine, parse_dates=['start_date', 'end_date'])
    df_product_universe['region'] = 'EMEA'
    df_product_universe.loc[df_product_universe['country_id'].isin([40, 234, 143]), 'region'] = 'AMER'
    # remove country_id
    df_product_universe = df_product_universe.drop(columns=['country_id'])

    # get SXXR and SPTR500N perf (product_id=845 for SXXR, 916 for SPTR500N Index)
    df_index = df_market[df_market['product_id'].isin([845, 916])]
    df_index = df_index.pivot_table(index='entry_date', columns='product_id', values='adj_price')
    df_index['SXXR'] = df_index[845].pct_change()
    df_index['SPTR500N'] = df_index[916].pct_change()
    df_index['BM'] = df_index['SPTR500N'] / 3 + df_index['SXXR'] * 2 / 3

    df_exposure = df_exposure.join(df_index[['SXXR', 'SPTR500N', 'BM']], how='outer')

    df_exposure['Net AMER Cash'] = df_exposure['Long AMER Cash'] + df_exposure['Short AMER Cash']
    df_exposure['Net EMEA Cash'] = df_exposure['Long EMEA Cash'] + df_exposure['Short EMEA Cash']

    df_exposure['Net AMER Cash %'] = df_exposure['Net AMER Cash'] / df_exposure['aum']
    df_exposure['Net AMER Future %'] = df_exposure['Short AMER Future'] / df_exposure['aum']
    df_exposure['Net AMER %'] = (df_exposure['Net AMER Cash'] + df_exposure['Short AMER Future']) / df_exposure['aum']

    df_exposure['Net EMEA Cash %'] = df_exposure['Net EMEA Cash'] / df_exposure['aum']
    df_exposure['Net EMEA Future %'] = df_exposure['Short EMEA Future'] / df_exposure['aum']
    df_exposure['Net EMEA %'] = (df_exposure['Net EMEA Cash'] + df_exposure['Short EMEA Future']) / df_exposure['aum']

    df_exposure['Net %'] = df_exposure['Net'] / df_exposure['aum']

    print('df_exposure done')

    # get % change for all AMER products
    df_market_amer = df_market[df_market['region'] == 'AMER']
    # pivot df_market_amer
    df_market_amer = df_market_amer.pivot_table(index='entry_date', columns='product_id', values='adj_price')
    # replace nan by None
    df_market_amer = df_market_amer.replace({np.nan: None})
    df_amer_pct = df_market_amer.pct_change()
    na_mask = df_market_amer.shift(1).isna()
    next_na_mask = df_market_amer.isna() & df_market_amer.shift(1).notna()
    df_amer_pct[na_mask] = None
    df_amer_pct[next_na_mask] = None
    # remove first row
    df_amer_pct = df_amer_pct.iloc[1:]
    print('df_market_amer done')

    # get % change for all EMEA products
    df_market_emea = df_market[df_market['region'] == 'EMEA']
    # pivot df_market_emea
    df_market_emea = df_market_emea.pivot_table(index='entry_date', columns='product_id', values='adj_price')
    # replace nan by None
    df_market_emea = df_market_emea.replace({np.nan: None})
    df_emea_pct = df_market_emea.pct_change()
    na_mask = df_market_emea.shift(1).isna()
    next_na_mask = df_market_emea.isna() & df_market_emea.shift(1).notna()
    df_emea_pct[na_mask] = None
    df_emea_pct[next_na_mask] = None
    # remove first row
    df_emea_pct = df_emea_pct.iloc[1:]
    print('df_market_emea done')

    # get list of df index from df_exposure
    date_list = df_exposure.index.tolist()
    # remove first element
    date_list = date_list[1:]

    # AMER
    df_exposure['Long AMER Universe'] = 0
    df_exposure['Short AMER Universe'] = 0

    df_exposure['AMER universe perf'] = 0

    df_position_amer = df_position[df_position['region'] == 'AMER']
    df_product_universe_amer = df_product_universe[df_product_universe['region'] == 'AMER']

    for date_str in date_list:
        # convert YYYY-MM in my_date to real date
        my_date = datetime.strptime(date_str, '%Y-%m')

        # filter df_product_universe_amer with start_date<= date  and (end_date==None or end_date>=date)
        df_product_universe_amer_filtered = df_product_universe_amer[(df_product_universe_amer['start_date'] <= my_date) & (
                (df_product_universe_amer['end_date'].isna()) | (df_product_universe_amer['end_date'] >= my_date))]
        # get list of product_id
        product_id_list = df_product_universe_amer_filtered['product_id'].tolist()

        # get position for the date
        df_position_amer_date = df_position_amer[df_position_amer['entry_date'] == date_str]
        # filter df_position_amer_date with product_id_list
        df_position_amer_date_filtered = df_position_amer_date[df_position_amer_date['product_id'].isin(product_id_list)]

        # df_exposure.loc[date_str, 'Long AMER Universe'] where side = 'Long'
        df_exposure.loc[date_str, 'Long AMER Universe'] = \
            df_position_amer_date_filtered[df_position_amer_date_filtered['side'] == 'Long']['mkt_value_usd'].sum()

        df_exposure.loc[date_str, 'Short AMER Universe'] = \
            df_position_amer_date_filtered[df_position_amer_date_filtered['side'] == 'Short']['mkt_value_usd'].sum()
        # if not last date

        # keep only columns in product_id_list for that date
        df_market_amer_date = df_amer_pct.loc[date_str, product_id_list]
        # get average performance
        perf = df_market_amer_date.mean()
        df_exposure.loc[date_str, 'AMER universe perf'] = perf

        print(f'AMER {date_str} done')

    # EMEA
    df_position_emea = df_position[df_position['region'] == 'EMEA']
    df_product_universe_emea = df_product_universe[df_product_universe['region'] == 'EMEA']

    df_exposure['Long EMEA Universe'] = 0
    df_exposure['Short EMEA Universe'] = 0

    df_exposure['EMEA universe perf'] = 0

    for date_str in date_list:
        # convert YYYY-MM in my_date to real date
        my_date = datetime.strptime(date_str, '%Y-%m')

        # filter df_product_universe_emea with start_date<= date and end_date==None or end_date>=date
        df_product_universe_emea_filtered = df_product_universe_emea[(df_product_universe_emea['start_date'] <= my_date) & (
                (df_product_universe_emea['end_date'].isna()) | (df_product_universe_emea['end_date'] >= my_date))]
        # get list of product_id
        product_id_list = df_product_universe_emea_filtered['product_id'].tolist()

        # get position for the date
        df_position_emea_date = df_position_emea[df_position_emea['entry_date'] == date_str]
        # filter df_position_emea_date with product_id_list
        df_position_emea_date_filtered = df_position_emea_date[df_position_emea_date['product_id'].isin(product_id_list)]

        # df_exposure.loc[date_str, 'Long EMEA Universe'] where side = 'Long'
        df_exposure.loc[date_str, 'Long EMEA Universe'] = \
            df_position_emea_date_filtered[df_position_emea_date_filtered['side'] == 'Long']['mkt_value_usd'].sum()

        df_exposure.loc[date_str, 'Short EMEA Universe'] = \
            df_position_emea_date_filtered[df_position_emea_date_filtered['side'] == 'Short']['mkt_value_usd'].sum()

        # keep only columns in product_id_list for that date
        df_market_emea_date = df_emea_pct.loc[date_str, product_id_list]
        # get average performance
        perf = df_market_emea_date.mean()
        df_exposure.loc[date_str, 'EMEA universe perf'] = perf

        print(f'EMEA {date_str} done')
    df_exposure['Universe Perf 33/66'] = df_exposure['AMER universe perf'] / 3 + 2 * df_exposure['EMEA universe perf'] / 3

    df_exposure['Net AMER Universe'] = df_exposure['Long AMER Universe'] + df_exposure['Short AMER Universe']
    df_exposure['Net EMEA Universe'] = df_exposure['Long EMEA Universe'] + df_exposure['Short EMEA Universe']
    df_exposure['Net AMER Universe %'] = df_exposure['Net AMER Universe'] / df_exposure['aum']
    df_exposure['Net EMEA Universe %'] = df_exposure['Net EMEA Universe'] / df_exposure['aum']
    df_exposure['Net Universe %'] = df_exposure['Net AMER Universe %'] + df_exposure['Net EMEA Universe %']

    # remove first row
    df_exposure = df_exposure.iloc[1:]

    # add Financing cost
    df_exposure['Financing Cost'] = df_exposure.index.map(financing_cost_dict)
    df_exposure['Financing Cost'] = df_exposure['Financing Cost'].fillna(method='ffill')
    df_exposure['Mngt Fee'] = -0.02 / 12

    df_exposure['Univ Over BM AMER'] = df_exposure['AMER universe perf'] - df_exposure['SPTR500N']
    df_exposure['Univ Over BM EMEA'] = df_exposure['EMEA universe perf'] - df_exposure['SXXR']

    df_exposure['AMER/EMEA split %'] = df_exposure['Net AMER %'] / df_exposure['Net %']
    df_exposure['AMER/EMEA Univ split %'] = df_exposure['Net AMER Universe %'] / df_exposure['Net Universe %']

    if net_perc:
        df_exposure['Net % Calc'] = net_perc
    else:
        df_exposure['Net % Calc'] = df_exposure['Net %']

    if amer_perc:
        df_exposure['AMER % Calc'] = amer_perc
        df_exposure['UNIV AMER % Calc'] = amer_perc
    else:
        df_exposure['AMER % Calc'] = df_exposure['AMER/EMEA split %']
        df_exposure['UNIV AMER % Calc'] = df_exposure['AMER/EMEA Univ split %']
    df_exposure['EMEA % Calc'] = 1 - df_exposure['AMER % Calc']
    df_exposure['UNIV EMEA % Calc'] = 1 - df_exposure['UNIV AMER % Calc']

    if universe_net_perc:
        df_exposure['UNIV NET % Calc'] = universe_net_perc

    else:
        df_exposure['UNIV NET % Calc'] = df_exposure['Net Universe %']

    df_exposure['BM Amer Perf'] = df_exposure['SPTR500N'] * df_exposure['AMER % Calc']

    df_exposure['Alto Perf'] = df_exposure['Nav Perf']
    df_exposure['BM AMER Perf'] = df_exposure['SPTR500N'] * df_exposure['AMER % Calc'] * df_exposure['Net % Calc']
    df_exposure['BM EMEA Perf'] = df_exposure['SXXR'] * df_exposure['EMEA % Calc'] * df_exposure['Net % Calc']
    df_exposure['BM Perf'] = df_exposure['BM AMER Perf'] + df_exposure['BM EMEA Perf']

    df_exposure['UNIV AMER Perf'] = df_exposure['Univ Over BM AMER'] * df_exposure['UNIV AMER % Calc'] * df_exposure['UNIV NET % Calc']
    df_exposure['UNIV EMEA Perf'] = df_exposure['Univ Over BM EMEA'] * df_exposure['UNIV EMEA % Calc'] * df_exposure['UNIV NET % Calc']
    df_exposure['UNIV Perf'] = df_exposure['UNIV AMER Perf'] + df_exposure['UNIV EMEA Perf']

    df_exposure['Fees Perf'] = df_exposure['Mngt Fee'] + df_exposure['Financing Cost']
    df_exposure['Idiosyncratic perf'] = df_exposure['Alto Perf'] - df_exposure['BM Perf'] - df_exposure['UNIV Perf'] - \
                                        df_exposure['Fees Perf']
    #export to excel
    df_exposure.to_excel('Excel\exposure.xlsx')

    # universe month by month
    # find list of stock universe for the month from df_product_universe


    # keep market data for both successive date in df_market
    # keep only stock from universe
    # pivot it and get return (remove missing value)


if __name__ == '__main__':

    start_date = date(2019, 4, 1)
    end_date = date.today()
    net_perc = None
    amer_perc = None
    universe_net_perc = None

    get_attribution(start_date, end_date, net_perc, amer_perc, universe_net_perc)