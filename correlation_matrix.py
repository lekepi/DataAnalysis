import pandas as pd
from models import engine
from datetime import date, timedelta


def calculate_correlation(start_date, end_date):
    my_sql = f"""SELECT entry_date,ticker,adj_price FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id
 WHERE prod_type='Cash' and entry_date>='{start_date}' and entry_date<='{end_date}' order by ticker,entry_date"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # remove week end
    df = df[df['entry_date'].dt.weekday < 5]
    df['return'] = df.groupby('ticker')['adj_price'].pct_change()
    df_pivot = df.pivot(index='entry_date', columns='ticker', values='return')
    correlation_matrix = df_pivot.corr()
    return correlation_matrix


if __name__ == '__main__':
    start_date = date.today() - timedelta(days=730)
    end_date = date.today()

    correlation_matrix = calculate_correlation(start_date, end_date)
    # export into excel
    correlation_matrix.to_excel('Excel\correlation_matrix.xlsx')


    pass