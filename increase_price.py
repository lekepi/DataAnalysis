import numpy as np
import pandas as pd
from models import engine, session, ProductBeta, Position
import time
from datetime import timedelta, date
from utils import clean_df_value, find_future_date, find_past_date


def get_increase():

    my_dict_list = []
    my_sql = "SELECT entry_date,count(id) as num FROM product_beta WHERE alpha>0.05 GROUP by entry_date;"
    df_cluster = pd.read_sql(my_sql, con=engine)

    # index return
    my_sql = f"""SELECT entry_date,adj_price as price FROM anandaprod.product_market_data WHERE product_id=437
                        and entry_date>='2019-01-01'"""
    df_spx = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    my_sql = f"""SELECT entry_date,adj_price as price FROM anandaprod.product_market_data WHERE product_id=439
                        and entry_date>='2019-01-01'"""
    df_sxxp = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    increase_list = session.query(ProductBeta).filter(ProductBeta.alpha > 0.05).\
        order_by(ProductBeta.entry_date.asc()).all()
    total = len(increase_list)
    count = 0
    for increase in increase_list:
        count += 1
        ticker = increase.product.ticker
        print(ticker)
        entry_date = increase.entry_date
        past_date_8w = entry_date - timedelta(days=56)
        past_date_1d = find_past_date(entry_date, 1)
        date_1d = find_future_date(entry_date, 1)
        date_2d = find_future_date(entry_date, 2)
        date_3d = find_future_date(entry_date, 3)
        date_1w = entry_date + timedelta(days=7)
        date_2w = entry_date + timedelta(days=14)
        date_4w = entry_date + timedelta(days=28)
        date_8w = entry_date + timedelta(days=56)
        cluster_num = df_cluster[df_cluster['entry_date'] == entry_date].iloc[0]['num']
        continent = increase.product.exchange.country.continent
        alpha = increase.alpha
        if increase.product.exchange.country.continent == 'AMER':
            df_index = df_spx
        else:
            df_index = df_sxxp

        position = session.query(Position).filter(Position.entry_date == entry_date).\
            filter(Position.product_id == increase.product.id).filter(Position.parent_fund_id == 1).first()

        if position:
            if position.quantity > 0:
                alto = 'B'
            elif position.quantity < 0:
                alto = 'S'
            else:
                alto = 'N'
        else:
            alto = 'N'

        # MA 50D
        my_sql = f"""SELECT entry_date,adj_price as price FROM anandaprod.product_market_data WHERE product_id={increase.product.id}
                    and entry_date<='{date_8w}' and entry_date>='{past_date_8w}';"""
        df_ma = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

        df_correl = df_ma[past_date_8w:entry_date]
        df_correl['log_ret'] = np.log1p(df_correl['price'].pct_change())
        stdev = df_correl['log_ret'].std()
        volatility = stdev * (252 ** 0.5)

        price_ma = df_ma.loc[past_date_8w:past_date_1d, 'price'].mean()
        price_previous = clean_df_value(df_ma.loc[past_date_1d:past_date_1d, 'price'])

        if price_previous > price_ma:
            px_above_ma = True
        else:
            px_above_ma = False

        price_past_8w = clean_df_value(df_ma.loc[past_date_8w:past_date_8w, 'price'])
        price = clean_df_value(df_ma.loc[entry_date:entry_date, 'price'])


        price_1d = None
        price_2d = None
        price_3d = None
        price_1w = None
        price_2w = None
        price_4w = None
        price_8w = None

        if date_1d<date.today(): price_1d = clean_df_value(df_ma.loc[date_1d:date_1d, 'price'])
        if date_2d<date.today(): price_2d = clean_df_value(df_ma.loc[date_2d:date_2d, 'price'])
        if date_3d<date.today(): price_3d = clean_df_value(df_ma.loc[date_3d:date_3d, 'price'])
        if date_1w<date.today(): price_1w = clean_df_value(df_ma.loc[date_1w:date_1w, 'price'])
        if date_2w<date.today(): price_2w = clean_df_value(df_ma.loc[date_2w:date_2w, 'price'])
        if date_4w<date.today(): price_4w = clean_df_value(df_ma.loc[date_4w:date_4w, 'price'])
        if date_8w<date.today(): price_8w = clean_df_value(df_ma.loc[date_8w:date_8w, 'price'])

        price_ind_past_8w = clean_df_value(df_index.loc[past_date_8w:past_date_8w, 'price'])
        price_previous_index = clean_df_value(df_index.loc[past_date_1d:past_date_1d, 'price'])
        price_ind = clean_df_value(df_index.loc[entry_date:entry_date, 'price'])

        price_ind_1d = None
        price_ind_2d = None
        price_ind_3d = None
        price_ind_1w = None
        price_ind_2w = None
        price_ind_4w = None
        price_ind_8w = None

        if date_1d<date.today(): price_ind_1d = clean_df_value(df_index.loc[date_1d:date_1d, 'price'])
        if date_2d<date.today(): price_ind_2d = clean_df_value(df_index.loc[date_2d:date_2d, 'price'])
        if date_3d<date.today(): price_ind_3d = clean_df_value(df_index.loc[date_3d:date_3d, 'price'])
        if date_1w<date.today(): price_ind_1w = clean_df_value(df_index.loc[date_1w:date_1w, 'price'])
        if date_2w<date.today(): price_ind_2w = clean_df_value(df_index.loc[date_2w:date_2w, 'price'])
        if date_4w<date.today(): price_ind_4w = clean_df_value(df_index.loc[date_4w:date_4w, 'price'])
        if date_8w<date.today(): price_ind_8w = clean_df_value(df_index.loc[date_8w:date_8w, 'price'])

        # beta
        my_sql = f"""SELECT entry_date,beta,alpha FROM product_beta WHERE product_id ={increase.product.id} 
                    and entry_date>='{past_date_8w}' and entry_date<='{date_8w}';"""
        df_beta = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

        beta = clean_df_value(df_beta.loc[entry_date:entry_date, 'beta'])
        if beta < 0:
            beta = 0

        alpha_past_8w = None
        alpha_1d = None
        alpha_2d = None
        alpha_3d = None
        alpha_1w = None
        alpha_2w = None
        alpha_4w = None
        alpha_8w = None
        alpha_cum_1d = None
        alpha_cum_2d = None
        alpha_cum_3d = None
        alpha_cum_1w = None
        alpha_cum_2w = None
        alpha_cum_4w = None
        alpha_cum_8w = None
        if price_past_8w:
            alpha_past_8w = (price_previous / price_past_8w - 1) - beta * (price_previous_index / price_ind_past_8w - 1)
        if price_1d:
            alpha_1d = (price_1d / price - 1) - beta * (price_ind_1d / price_ind - 1)
            alpha_cum_1d = df_beta.loc[date_1d:date_1d, 'alpha'].sum()
        if price_2d:
            alpha_2d = (price_2d / price - 1) - beta * (price_ind_2d / price_ind - 1)
            alpha_cum_2d = df_beta.loc[date_1d:date_2d, 'alpha'].sum()
        if price_3d:
            alpha_3d = (price_3d / price - 1) - beta * (price_ind_3d / price_ind - 1)
            alpha_cum_3d = df_beta.loc[date_1d:date_3d, 'alpha'].sum()
        if price_1w:
            alpha_1w = (price_1w / price - 1) - beta * (price_ind_1w / price_ind - 1)
            alpha_cum_1w = df_beta.loc[date_1d:date_1w, 'alpha'].sum()
        if price_2w:
            alpha_2w = (price_2w / price - 1) - beta * (price_ind_2w / price_ind - 1)
            alpha_cum_2w = df_beta.loc[date_1d:date_2w, 'alpha'].sum()
        if price_4w:
            alpha_4w = (price_4w / price - 1) - beta * (price_ind_4w / price_ind - 1)
            alpha_cum_4w = df_beta.loc[date_1d:date_4w, 'alpha'].sum()
        if price_8w:
            alpha_8w = (price_8w / price - 1) - beta * (price_ind_8w / price_ind - 1)
            alpha_cum_8w = df_beta.loc[date_1d:date_8w, 'alpha'].sum()

        my_dict = {
            'Entry_date': entry_date,
            'Ticker': ticker,
            'Alpha': alpha,
            'Continent': continent,
            'a_past_8w': alpha_past_8w,
            'Px_prev': price_previous,
            'Px': price,
            'Px_MA': price_ma,
            'Px_Above_MA': px_above_ma,
            'Cluster_Num': cluster_num,
            'Alto': alto,
            'Beta': beta,
            'Vol': volatility,
            'a_1d': alpha_1d,
            'a_2d': alpha_2d,
            'a_3d': alpha_3d,
            'a_1w': alpha_1w,
            'a_2w': alpha_2w,
            'a_4w': alpha_4w,
            'a_8w': alpha_8w,
            'a_cum_1d': alpha_cum_1d,
            'a_cum_2d': alpha_cum_2d,
            'a_cum_3d': alpha_cum_3d,
            'a_cum_1w': alpha_cum_1w,
            'a_cum_2w': alpha_cum_2w,
            'a_cum_4w': alpha_cum_4w,
            'a_cum_8w': alpha_cum_8w,
        }
        my_dict_list.append(my_dict)

        print(f'{round(count/total*100,2)}%')

    df = pd.DataFrame(my_dict_list)
    df.to_csv("H:\Python Output\increase_price.csv", index=False)


if __name__ == '__main__':
    start = time.time()
    get_increase()
    duration =round(time.time() - start, 2)
    # print('It took', duration, 'seconds.')

