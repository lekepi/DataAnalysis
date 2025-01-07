import pandas as pd
from models import engine, session, NavAccountStatement
from utils import last_alpha_date


def get_alpha_long(region):
    # Long notional
    if region == 'Europe':
        sql_extra = " and T4.continent!='AMER'"
    elif region == 'All':
        sql_extra = ""
    my_sql = f""" SELECT T1.entry_date,sum(T1.alpha_usd) as alpha_usd, sum(T1.mkt_value_usd) as notional_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id LEFT JOIN exchange T3 on T2.exchange_id=T3.id 
        LEFT JOIN country T4 on T3.country_id=T4.id WHERE T2.prod_type = 'Cash'
        {sql_extra} and T1.parent_fund_id=1 and entry_date>='2019-04-01' and T1.quantity>0
        group by T1.entry_date Order by T1.entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df['alpha_bp'] = df['alpha_usd'] / df['notional_usd'] * 10000

    # save excel
    df.to_excel(f'H:/Python Output/Alto Long Alpha {region}.xlsx')

    # trading
    my_sql = f"""SELECT trade_date as entry_date,
  SUM(CASE WHEN quantity > 0 THEN notional_usd ELSE 0 END) AS buy_usd,
  SUM(CASE WHEN quantity < 0 THEN notional_usd ELSE 0 END) AS sell_usd
FROM trade T1 JOIN product T2 ON T1.product_id = T2.id LEFT JOIN exchange T3 on T2.exchange_id=T3.id 
        LEFT JOIN country T4 on T3.country_id=T4.id
WHERE T2.prod_type = 'cash' AND trade_date >= '2019-04-01' AND position_side = 'Long'
and quantity<>0 and T1.parent_fund_id=1 {sql_extra} GROUP BY trade_date;"""
    df_trading = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_trading['gross_usd'] = df_trading['buy_usd'] - df_trading['sell_usd']

    # group df_trading per year (sum)
    df_trading = df_trading.groupby(pd.Grouper(freq='Y')).sum()
    # change index to year only
    df_trading.index = df_trading.index.year

    df_avg_long = df.groupby(pd.Grouper(freq='Y')).mean()
    # keep only notional_usd
    df_avg_long = df_avg_long[['notional_usd']]
    # change index to year only
    df_avg_long.index = df_avg_long.index.year

    # merge df_trading and df_avg_long
    df_trading = pd.merge(df_avg_long, df_trading, left_index=True, right_index=True)

    # save excel (use region in name), save in H drive, Python Output folder
    df_trading.to_excel(f'H:/Python Output/Alto Long Trading {region}.xlsx')

    # sector

    # get list of start date of the month
    my_sql = """SELECT MIN(entry_date) AS first_date FROM position WHERE entry_date>='2019-04-01' GROUP BY YEAR(entry_date), MONTH(entry_date)"""
    df_date = pd.read_sql(my_sql, con=engine)
    first_date_list = df_date['first_date'].tolist()
    last_date = last_alpha_date()
    first_date_list.append(last_date)

    first_date_str = ','.join([f"'{first_date}'" for first_date in first_date_list])

    my_sql = f"""SELECT T1.entry_date,T5.name as sector, sum(T1.mkt_value_usd) as notional_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id LEFT JOIN exchange T3 on T2.exchange_id=T3.id 
        LEFT JOIN country T4 on T3.country_id=T4.id LEFT JOIN reporting_sector T5 on T5.id=T2.reporting_sector_id 
        WHERE T2.prod_type = 'Cash' {sql_extra}
        and T1.parent_fund_id=1 and entry_date in ({first_date_str}) and T1.quantity>0 
        group by T1.entry_date,T5.name Order by T1.entry_date;"""
    df_sector_raw = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # Delete when df['sector'] is null
    df_sector_raw = df_sector_raw[df_sector_raw['sector'].notnull()]
    # find max entry_date in df_sector_raw
    max_date = df_sector_raw['entry_date'].max()
    df_sector_last = df_sector_raw[df_sector_raw['entry_date'] == max_date]
    # divide notional_usd by sum(notional_usd)
    df_sector_last['notional_usd'] = df_sector_last['notional_usd'] / df_sector_last['notional_usd'].sum()

    # save excel
    df_sector_last.to_excel(f'H:/Python Output/Alto Long Sector Last {region}.xlsx')

    # pivot by sector
    df_sector = df_sector_raw.pivot(index='entry_date', columns='sector', values='notional_usd')

    # divide each line by the sum of the line
    df_sector = df_sector.div(df_sector.sum(axis=1), axis=0)
    # fill NaN with 0
    df_sector = df_sector.fillna(0)
    # save excel
    df_sector.to_excel(f'H:/Python Output/Alto Long Sector {region}.xlsx')
    pass


if __name__ == '__main__':
    get_alpha_long('Europe')
    get_alpha_long('All')
