import pandas as pd
from models import engine


def get_market_cap_analysis():
    my_sql = """SELECT T2.id,T2.ticker,year(entry_date) as year FROM position T1 JOIN product T2 on T1.product_id=T2.id 
WHERE T2.prod_type='Cash' and entry_date<'2023-01-01' group by T2.id,T2.ticker,year(entry_date) order by T2.id,year(entry_date);"""
    df = pd.read_sql(my_sql, con=engine)

    my_sql = """SELECT product_id,year(entry_date) as year,market_cap FROM product_market_cap;"""
    df2 = pd.read_sql(my_sql, con=engine)

    df3 = pd.merge(df, df2, how='left', left_on=['id', 'year'], right_on=['product_id', 'year'])

    # keep when market_cap is null
    df4 = df3[df3['market_cap'].isnull()]
    df4 = df4[['id', 'ticker', 'year']]

    # store in excel
    df4.to_excel(rf'H:\TEMP\market_cap_analysis.xlsx', index=False)


if __name__ == '__main__':
    get_market_cap_analysis()
