from datetime import date
from models import session, engine
import pandas as pd


def get_alpha_stock(start_date, end_date):
    my_sql = f"""SELECT entry_date,ticker,alpha,prod_type FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE entry_date>='{start_date}' and entry_date<='{end_date}'"""

    df = pd.read_sql(my_sql, con=engine)
    #  filter keep only prod_type='Cash'
    df = df[df['prod_type'] == 'Cash']
    # group by ticker, sum alpha, count(row)
    df_group = df.groupby('ticker').agg({'alpha': 'sum', 'prod_type': 'count'})

    print(1)


if __name__ == '__main__':
    start_date = date(2019, 4, 1)
    end_date = date.today()
    get_alpha_stock(start_date, end_date)



