
from models import ProductAction, session, ProductMarketData
from utils import find_past_date
import pandas as pd

if __name__ == '__main__':
    product_action_list = session.query(ProductAction).all()

    df = pd.DataFrame(columns=["id", "entry_date", "ticker", "price"])
    count = 0
    for product_action in product_action_list:
        my_date = product_action.entry_date
        date_1d = find_past_date(my_date, 1)
        date_2d = find_past_date(date_1d, 1)

        date_list = [my_date, date_1d, date_2d]
        for date in date_list:
            pmd = session.query(ProductMarketData).filter(ProductMarketData.product_id == product_action.product_id)\
                .filter(ProductMarketData.entry_date == date).first()
            if pmd:
                price = pmd.price
                id = pmd.id

                # add line into df
                data = {'id': id, 'entry_date': date, 'ticker': product_action.product.ticker, 'price': price}
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
        count += 1
        print(count)
    df.to_csv('correct_actual_price.csv', index=False)
