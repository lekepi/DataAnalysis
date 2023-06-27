
# find list of stocks that has the same price for 5 successive days or more. To find ticker that are not
# really active anymore, and remove these date from product_market_data

import pandas as pd
from models import engine


def get_df():
    my_sql = "SELECT entry_date,product_id,adj_price FROM product_market_data order by product_id,entry_date"
    df = pd.read_sql(my_sql, con=engine)
    # find when 5 times the same price in a row for a specific product
    # Create a new column to check if the price is the same as the previous row
    df['price_same'] = (df['adj_price'] == df['adj_price'].shift(1))

    # keep rows only if df['price_same'] is True 5 times in a row
    df = df[df['price_same'].rolling(5).sum() == 5]

    print(1)


if __name__ == '__main__':
    get_df()
