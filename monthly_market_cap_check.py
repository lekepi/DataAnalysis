import pandas as pd
from models import engine


if __name__ == '__main__':
    my_sql = """SELECT MIN(entry_date) AS first_date FROM position WHERE entry_date>='2019-04-01' GROUP BY YEAR(entry_date), MONTH(entry_date)"""
    df = pd.read_sql(my_sql, con=engine)

    # put the first date of each month into a list
    first_date_list = df['first_date'].tolist()

    # create a string of first date in quotes
    first_date_str = ''
    for first_date in first_date_list:
        first_date_str += f"'{first_date}',"
    first_date_str = first_date_str[:-1]
    print(first_date_str)

    my_sql = f"""select T1.entry_date,T1.product_id, T2.ticker,T3.market_cap from position T1 
join product T2 on T1.product_id=T2.id
LEFT join product_market_cap T3 on T1.product_id=T3.product_id and T1.entry_date=T3.entry_date and T3.type='Monthly'
 where parent_fund_id=1 and prod_type='Cash' and T1.entry_date in ({first_date_str})
 and T3.market_cap is NULL"""

    df = pd.read_sql(my_sql, con=engine)
    if not df.empty:
        print(df)
    else:
        print("All Good")
