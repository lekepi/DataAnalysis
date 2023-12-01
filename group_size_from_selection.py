from datetime import date
from models import session, ProductGroup, engine
import pandas as pd


def get_product_id_list(group_name):
    product_group_list = session.query(ProductGroup).filter(ProductGroup.group_name == group_name).all()
    stock_id_list = []
    for product_group in product_group_list:
        stock_id_list.append(product_group.product_id)

    return stock_id_list


if __name__ == '__main__':
    group_name = 'Beverages'
    start_date = date(2019, 4, 1)
    end_date = date.today()

    # get all dates
    my_sql = f"""SELECT distinct(entry_date),1 FROM alpha_summary WHERE entry_date>='{start_date}' and entry_date<='{end_date}' order by entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col=['entry_date'])

    stock_id_list = get_product_id_list(group_name)
    stock_id_list_str = str(stock_id_list).replace('[', '(').replace(']', ')')

    my_sql = f"""SELECT last_date,sum(current_size) as current_size FROM anandaprod.analyst_perf where product_id in 
    {stock_id_list_str} and is_historic=0 and is_top_pick=0 group by last_date order by last_date;"""

    df_current_size = pd.read_sql(my_sql, con=engine, parse_dates=['last_date'], index_col=['last_date'])
    # merge
    df = pd.merge(df, df_current_size, how='left', left_index=True, right_index=True)
    first_non_null_index = df['current_size'].first_valid_index()
    if first_non_null_index is not None:
        df = df.loc[first_non_null_index:]
    df = df.fillna(0)

    print(stock_id_list_str)
