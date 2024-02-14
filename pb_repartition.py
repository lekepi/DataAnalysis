import pandas as pd
from models import engine, session, Product, ExecFee, TaskChecker

if __name__ == '__main__':
    my_sql = """SELECT mkt_value_usd,
                mkt_value_usd*qty_gs/(qty_gs+qty_ubs) as mkt_value_usd_gs,
                mkt_value_usd*qty_ubs/(qty_gs+qty_ubs) as mkt_value_usd_ubs,
                T2.prod_type,T4.name as country FROM position T1 JOIN product T2 on T1.product_id=T2.id JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id 
                WHERE entry_date='2024-01-26' and parent_fund_id=1 order by mkt_value_usd"""
    df = pd.read_sql(my_sql, con=engine)
    df_long = df[df['mkt_value_usd'] > 0]
    df_short = df[df['mkt_value_usd'] < 0]


    pass