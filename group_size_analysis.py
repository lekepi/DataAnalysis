from datetime import date
from models import engine, ProductGroup, session
import pandas as pd


def get_group_size_analysis(group_name, start_date, end_date):

    stock_list_db = session.query(ProductGroup).filter(ProductGroup.group_name == group_name).all()
    ticker_list = [pg.product.ticker for pg in stock_list_db]
    ticker_list_str = ', '.join([f"'{ticker}'" for ticker in ticker_list])

    my_sql = f"""SELECT entry_date,T2.ticker,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE 
    parent_fund_id=1 and T2.ticker in ({ticker_list_str}) and entry_date>='{start_date}' and entry_date<='{end_date}'; """

    df = pd.read_sql(my_sql, con=engine)
    # pivot by ticker
    df = df.pivot(index='entry_date', columns='ticker', values='mkt_value_usd')
    df.fillna(0, inplace=True)
    # add total column
    df['Total'] = df.sum(axis=1)
    # save into the excel folder
    df.to_excel(rf'Excel\{group_name}_size.xlsx', index=True, sheet_name='Group Size')


if __name__ == '__main__':
    group_name = 'biosimilar'
    start_date = date(2024, 6, 15)
    end_date = date.today()

    get_group_size_analysis(group_name, start_date, end_date)
