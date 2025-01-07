from models import session, Position, Product, engine
import pandas as pd


if __name__ == '__main__':

    # '2024-11-11'
    # '2024-11-12'

    my_sql = """SELECT entry_date,T3.name as strategy,sum(mkt_value_usd) as notional_usd,sum(alpha_usd) as alpha_usd 
                 FROM position T1 JOIN product T2 on T1.product_id=T2.id JOIN product_strategy T3 on 
                 T2.strategy_id=T3.id WHERE parent_fund_id=1 and prod_type='Cash' and entry_date in('2024-11-11', '2024-11-12')
                 group by entry_date,T3.name;"""

    df_strat = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    print(df_strat)