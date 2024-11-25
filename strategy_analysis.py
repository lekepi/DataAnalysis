from models import session, engine, ProductStrategy
import pandas as pd


def get_strategy_stats():

    my_sql = """SELECT p.entry_date, ps.name as strategy FROM product_strategy ps CROSS JOIN (SELECT DISTINCT entry_date FROM position) p 
        WHERE entry_date>='2019-04-01' ORDER BY p.entry_date, ps.name;"""
    df_date = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = f"""SELECT entry_date,T3.name as strategy,sum(mkt_value_usd) as notional_usd,sum(alpha_usd) as alpha_usd 
                     FROM position T1 JOIN product T2 on T1.product_id=T2.id JOIN product_strategy T3 on 
                     T2.strategy_id=T3.id WHERE parent_fund_id=1 and prod_type='Cash' and entry_date>='2019-04-01'
                     group by entry_date,T3.name;"""
    df_strat = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # merge df_date and df_strat on entry_date and strategy
    df_strat = df_date.merge(df_strat, how='left', left_on=['entry_date', 'strategy'],
                             right_on=['entry_date', 'strategy'])
    # fill na with 0
    df_strat = df_strat.fillna(0)

    # Long notional
    my_sql = f"""SELECT T1.entry_date,sum(T1.mkt_value_usd) as long_usd FROM position T1
        JOIN product T2 on T1.product_id=T2.id WHERE T2.prod_type = 'Cash' and T1.quantity>0
        and T1.parent_fund_id=1 and entry_date>='2019-04-01' group by T1.entry_date
        Order by T1.entry_date;"""
    df_long = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')
    df_strat = df_strat.merge(df_long, how='left', left_on='entry_date', right_index=True)
    df_strat['alpha Alto BP'] = df_strat['alpha_usd'] / df_strat['long_usd'] * 10000
    df_strat['size %'] = df_strat['notional_usd'] / df_strat['long_usd'] * 100

    # put df_strat into excel
    df_strat.to_excel('Excel/strategy_stats.xlsx', index=False)


if __name__ == '__main__':
    get_strategy_stats()
