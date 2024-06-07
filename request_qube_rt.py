from datetime import date, timedelta
import pandas as pd
from models import engine, session, Aum


def get_qsma_daily_pnl():
    columns = ['Portfolio Name', 'Date', 'GrossPnL', 'GrossRet%', 'GrossExposure', 'LongExposure',
               'ShortExposure', 'SingleName Short', 'IndexShort', 'NAV/AUM', 'GMVMultiplier', 'Currency', 'ShortRet%',
               'LongRet%', 'SingleName Short%', 'IndexShort %']

    my_sql = f"""SELECT entry_date,CASE when quantity>0 then 'Long' else 'Short' END as side,prod_type,
sum(pnl_usd) as pnl_usd,sum(abs(mkt_value_usd)) as notional_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id
WHERE entry_date>='2019-04-01' and parent_fund_id=1 and prod_type in ('Cash','Future') and T2.ticker not in 
('AGI US', 'FNV US','FNV CN','NEM US','GOLD US','AEM US','GDX US','GC1 CMX','GLD US', 'ED1 CME', 'TY1 CBT')
group by entry_date,side,prod_type"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # Calculate the aggregations separately
    agg_notional_long = df.loc[df['side'] == 'Long'].groupby('entry_date')['notional_usd'].sum()
    agg_notional_short = df.loc[df['side'] == 'Short'].groupby('entry_date')['notional_usd'].sum()
    agg_notional_index_short = df.loc[(df['side'] == 'Short') & (df['prod_type'] == 'Future')].groupby('entry_date')['notional_usd'].sum()
    agg_notional_single_short = df.loc[(df['side'] == 'Short') & (df['prod_type'] == 'Cash')].groupby('entry_date')['notional_usd'].sum()
    agg_pnl = df.groupby('entry_date')['pnl_usd'].sum()
    agg_pnl_long = df.loc[df['side'] == 'Long'].groupby('entry_date')['pnl_usd'].sum()
    agg_pnl_short = df.loc[df['side'] == 'Short'].groupby('entry_date')['pnl_usd'].sum()
    agg_pnl_index_short = df.loc[(df['side'] == 'Short') & (df['prod_type'] == 'Future')].groupby('entry_date')['pnl_usd'].sum()
    agg_pnl_single_short = df.loc[(df['side'] == 'Short') & (df['prod_type'] == 'Cash')].groupby('entry_date')['pnl_usd'].sum()

    # Create the final DataFrame by merging the aggregations
    df_output = pd.DataFrame({
        'GrossExposure': agg_notional_long + agg_notional_short,
        'LongExposure': agg_notional_long,
        'ShortExposure': agg_notional_short,
        'SingleName Short': agg_notional_single_short,
        'IndexShort': agg_notional_index_short,
        'GrossPnL': agg_pnl,
        'GrossPnL_long': agg_pnl_long,
        'GrossPnL_short': agg_pnl_short,
        'GrossPnL_index_short': agg_pnl_index_short,
        'GrossPnL_single_short': agg_pnl_single_short
    }).reset_index()

    my_sql = "SELECT entry_date,round(amount,1)*1000000 as aum from AUM WHERE type='leveraged' and fund_id=4 order by entry_date"
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # merge on entry_date
    df_output = pd.merge(df_output, df_aum, how='left', on='entry_date')
    # fill na with previous
    df_output['aum'] = df_output['aum'].fillna(method='ffill')
    # rename aum to NAV/AUM
    df_output.rename(columns={'aum': 'NAV/AUM'}, inplace=True)

    df_output['Portfolio Name'] = 'Alto'
    # reformat entry_date with DD/MM/YYYY
    df_output['Date'] = df_output['entry_date'].dt.strftime('%d/%m/%Y')
    df_output['GrossRet%'] = df_output['GrossPnL'] / df_output['GrossExposure']
    df_output['GMVMultiplier'] = df_output['GrossExposure'] / df_output['NAV/AUM']
    df_output['Currency'] = 'USD'
    df_output['ShortRet%'] = df_output['GrossPnL_short'] / df_output['GrossExposure']
    df_output['LongRet%'] = df_output['GrossPnL_long'] / df_output['GrossExposure']
    df_output['SingleName Short%'] = df_output['GrossPnL_single_short'] / df_output['GrossExposure']
    df_output['IndexShort %'] = df_output['GrossPnL_index_short'] / df_output['GrossExposure']

    # reorder columns and keep only columns in columns
    df_output = df_output[columns]

    #export in excel without index
    df_output.to_excel('qsma_daily_pnl.xlsx', index=False)


def get_start_month_weekday_list():

    start_date = date(2020, 4, 1)
    date_list = [start_date]
    final_day = date(2023, 4, 3)

    while start_date < final_day:
        start_date = start_date.replace(day=1) + timedelta(days=32)
        start_date = start_date.replace(day=1)
        # if weekend go forward to the monday
        if start_date.weekday() == 5:
            start_date = start_date + timedelta(days=2)
        elif start_date.weekday() == 6:
            start_date = start_date + timedelta(days=1)

        if start_date == date(2021, 1, 1):
            start_date = date(2021, 1, 4)

        date_list.append(start_date)

    return date_list


def get_start_month_position():
    date_list = get_start_month_weekday_list()

    # I want to have a string with the list of date separated with commas
    date_list = ','.join([f"'{date}'" for date in date_list])

    my_sql = f"""SELECT entry_date,T2.ticker as BBG,T2.sedol,T2.isin,T4.name as country,quantity,mkt_value_usd,T5.name as sector
                FROM position T1 JOIN product T2 on T1.product_id=T2.id JOIN exchange T3 on T2.exchange_id=T3.id 
                JOIN country T4 on T3.country_id=T4.id JOIN industry_group T5 on T2.industry_group_id=T5.id 
                WHERE entry_date in ({date_list}) and parent_fund_id=1 and prod_type in ('Cash','Future') 
                and T2.ticker not in ('AGI US', 'FNV US','FNV CN','NEM US','GOLD US','AEM US','GDX US','GC1 CMX','GLD US', 'ED1 CME', 'TY1 CBT')
                order by entry_date,mkt_value_usd desc;"""
    df = pd.read_sql(my_sql, con=engine)

    my_sql = """SELECT entry_date,amount*1000000 as nav_usd FROM aum WHERE type='leveraged' and fund_id=4 and 
                entry_date>='2020-03-01' and entry_date<'2023-05-01';"""
    df_aum = pd.read_sql(my_sql, con=engine)
    # merge on entry_date
    df = pd.merge(df, df_aum, how='left', left_on='entry_date', right_on='entry_date')

    # reformat Date to be dd/mm/yyyy
    df['Date'] = pd.to_datetime(df['entry_date']).dt.strftime('%d/%m/%Y')

    # replace symbol 'SXO1 EUX' with 'SXO1 Index'
    df['BBG'] = df['BBG'].replace('SXO1 EUX', 'SXO1 Index')
    # replace symbol 'ES1 CME' with 'ES1 Index'
    df['BBG'] = df['BBG'].replace('ES1 CME', 'ES1 Index')

    df['Instrument Type'] = 'Equity'
    df['Reporting Currency Code'] = 'USD'
    df['CUSIP'] = 'N/A'
    df['Custom Identifier'] = 'N/A'
    df['RIC'] = 'N/A'

    # rename entry_date into date, sedol into SEDOL
    df.rename(columns={'sedol': 'SEDOL', 'isin': 'ISIN',
                       'country': 'Listing Country', 'sector': 'Listing Sector',
                       'quantity': 'EOD Shares/Qty', 'nav_usd': 'EOD Portfolio Total AUM/NAV',
                       'mkt_value_usd': 'EOD Net Notional Value'}, inplace=True)

    columns = ['Date', 'BBG', 'RIC', 'SEDOL', 'ISIN', 'CUSIP', 'Custom Identifier', 'Instrument Type', 'Listing Country',
               'Listing Sector', 'EOD Shares/Qty', 'EOD Net Notional Value', 'EOD Portfolio Total AUM/NAV',
               'Reporting Currency Code']

    # reorder columns
    df = df[columns]

    # export in excel without index
    df.to_excel('qube_rt_monthly_position.xlsx', index=False)


if __name__ == '__main__':
    # get_qsma_daily_pnl()
    get_start_month_position()
