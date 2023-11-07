import pandas as pd
from datetime import date, timedelta
from models import engine, session ,Product

def find_previous_date(my_date):
    if my_date.weekday() == 0:
        previous_date = my_date - timedelta(days=3)
    elif my_date.weekday() == 6:
        previous_date = my_date - timedelta(days=2)
    else:
        previous_date = my_date - timedelta(days=1)
    return previous_date


def get_first_day_of_next_month(my_date):
    my_date = my_date.replace(day=1)
    my_date = my_date + timedelta(days=32)
    my_date = my_date.replace(day=1)
    return my_date


def get_weekday(my_date):
    if my_date.weekday() == 5:
        my_date = my_date - timedelta(days=1)
    elif my_date.weekday() == 6:
        my_date = my_date - timedelta(days=2)
    return my_date


if __name__ == '__main__':

    # % Invest = Long / AUM = perc_invested (88% per default)
    # Net Expo = (Long + Short) / AUM * 2 = net_expo (80% per default)

    net_exposure = 80
    perc_invested = 88
    fees = 3
    start_date_pos = date(2020, 2, 1)
    start_date_pos = get_weekday(start_date_pos)
    start_date_px = find_previous_date(start_date_pos)

    date_list = [start_date_px]

    current_date = start_date_px
    while current_date < date.today():
            current_date = get_first_day_of_next_month(current_date)
            current_date = get_first_day_of_next_month(current_date)
            current_date = find_previous_date(current_date)
            if current_date < date.today():
                date_list.append(current_date)
            previous_day = find_previous_date(current_date)

    if previous_day not in date_list:
        date_list.append(previous_day)
    # get the list of date in a string with comma separated
    date_list_str = ["'" + str(x) + "'" for x in date_list]
    date_list_str = ",".join(date_list_str)

    # get alto position for the start_date
    my_sql = f"""SELECT product_id,mkt_value_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id
     WHERE parent_fund_id=1 and entry_date='{start_date_pos}' and prod_type in ('cash','future');"""
    df_pos = pd.read_sql(my_sql, con=engine)

    # get the list of product_id in a string
    product_list = [str(x) for x in df_pos['product_id'].tolist()]
    product_list = ",".join(product_list)

    my_sql = f"""SELECT product_id,entry_date,adj_price FROM product_market_data WHERE entry_date in 
    ({date_list_str}) and product_id in ({product_list});"""
    df_price = pd.read_sql(my_sql, con=engine)
    df_pos['price1'] = None
    df_pos['price2'] = None

    for index, date in enumerate(date_list):
        price_dict = df_price[df_price['entry_date'] == date].set_index('product_id')['adj_price'].to_dict()
        if index == 0:
            # calculate df_pos['price1'] from df_price for date:
            df_pos['price2'] = df_pos['product_id'].map(price_dict)
            # long_total when 'mkt_value_usd' is positive, short_total when 'mkt_value_usd' is negative
            long_total = df_pos[df_pos['mkt_value_usd'] > 0]['mkt_value_usd'].sum()
            short_total = df_pos[df_pos['mkt_value_usd'] < 0]['mkt_value_usd'].sum()
            net_total = long_total + short_total
            aum = net_total / net_exposure * 200
            long_theo = aum * perc_invested / 100
            short_theo = net_exposure*aum/200 - long_theo
            long_multi = long_theo / long_total
            short_multi = short_theo / short_total
            df_pos['mkt_value_usd'] = df_pos['mkt_value_usd'].apply(lambda x: x * long_multi if x > 0 else x * short_multi)
            df_pos['mkt_value_usd2'] = df_pos['mkt_value_usd']
        else:
            df_pos['price1'] = df_pos['price2']
            df_pos['mkt_value_usd'] = df_pos['mkt_value_usd2']
            df_pos['price2'] = df_pos['product_id'].map(price_dict)
            # when the price is not available, use the previous price
            # print when the price is not available

            # get the list of product_id with missing price
            missing_price_list = df_pos[df_pos['price2'].isnull()]['product_id'].tolist()
            if len(missing_price_list) > 0:
                for missing_product_id in missing_price_list:
                    ticker = session.query(Product).filter(Product.id == missing_product_id).first().ticker
                    # get lat price for the missing product_id
                    my_sql = f"""SELECT product_id,entry_date,adj_price FROM product_market_data T1
                    WHERE entry_date < '{date}' and entry_date> '{previous_date}' and 
                    product_id = {missing_product_id} ORDER BY entry_date DESC LIMIT 1;"""
                    df_missing_price = pd.read_sql(my_sql, con=engine)
                    if len(df_missing_price) > 0:
                        df_pos.loc[df_pos['product_id'] == missing_product_id, 'price2'] = df_missing_price['adj_price'].values[0]
                        print(f"Product {ticker}, value from middle of the month is used for {date}")
                    else:
                        # delete the row in df_pos
                        df_pos = df_pos[df_pos['product_id'] != missing_product_id]
                        print(f"Product {ticker}, id={missing_product_id} is not available for {date}")

            df_pos['mkt_value_usd2'] = df_pos['mkt_value_usd'] * df_pos['price2'] / df_pos['price1']
            start_long_total = df_pos[df_pos['mkt_value_usd'] > 0]['mkt_value_usd'].sum()
            start_short_total = df_pos[df_pos['mkt_value_usd'] < 0]['mkt_value_usd'].sum()
            start_aum = aum
            fees_usd = aum * fees / (100*12)
            old_gross_total = start_long_total - start_short_total
            df_pos['mkt_value_usd2'] = df_pos['mkt_value_usd2'] - fees_usd * abs(df_pos['mkt_value_usd']) / old_gross_total

            # calculate the perf of the month
            monthly_pnl = df_pos['mkt_value_usd2'].sum() - df_pos['mkt_value_usd'].sum()
            monthly_perf = monthly_pnl / start_aum * 2  # Alto is 200%
            print(f"Monthly perf for {date} is {round(100*monthly_perf,2)}")

            # then do the rebalancing
            aum = start_aum + monthly_pnl
            new_long_total = df_pos[df_pos['mkt_value_usd2'] > 0]['mkt_value_usd2'].sum()
            new_short_total = df_pos[df_pos['mkt_value_usd2'] < 0]['mkt_value_usd2'].sum()

            long_theo = aum * perc_invested / 100
            short_theo = net_exposure * aum / 200 - long_theo
            long_multi = long_theo / new_long_total
            short_multi = short_theo / new_short_total
            df_pos['mkt_value_usd2'] = df_pos['mkt_value_usd2'].apply(lambda x: x * long_multi if x > 0 else x * short_multi)

        previous_date = date





