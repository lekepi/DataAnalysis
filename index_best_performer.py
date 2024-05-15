import pandas as pd
from models import session, engine


def get_average_position_alto(full_path, index):
    # read excel into df
    df_best = pd.read_excel(full_path, sheet_name=index)
    df_best['Expo %'] = None

    my_sql = "SELECT entry_date,long_usd FROM alpha_summary WHERE parent_fund_id=1 and entry_date>='2019-04-01' order by entry_date;"
    df_long = pd.read_sql(my_sql, engine, parse_dates=['entry_date'], index_col='entry_date')

    # get different year
    year_list = df_best['Year'].unique()

    for year in year_list:
        df_long_year = df_long[df_long.index.year == year]
        df_best_year = df_best[df_best['Year'] == year]

        # get all tickers
        tickers = df_best_year['Ticker'].unique()
        for ticker in tickers:
            my_sql = f"""SELECT entry_date,mkt_value_usd as notional_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
            WHERE T2.ticker='{ticker}' and parent_fund_id=1 and mkt_value_usd is NOT NULL;"""
            df_position = pd.read_sql(my_sql, engine, parse_dates=['entry_date'], index_col='entry_date')

            # left join df_long_year and df_position
            df_stock = df_long_year.join(df_position, how='left')
            # fill na with 0
            df_stock['notional_usd'] = df_stock['notional_usd'].fillna(0)
            # calculate %
            df_stock['expo %'] = df_stock['notional_usd'] / df_stock['long_usd']
            # calculate the average position
            average_position = df_stock['expo %'].mean()

            # complete df_best for that year and ticker
            df_best.loc[(df_best['Ticker'] == ticker) & (df_best['Year'] == year), 'Expo %'] = average_position

    # save df_best to excel is excel folder
    df_best.to_excel(f'Excel\Index - Best stock performer per year_{index}.xlsx', index=False)


if __name__ == '__main__':

    full_path = r"H:\Genia\Index - Best stock performer per year.xlsx"
    get_average_position_alto(full_path, 'SPX')
    get_average_position_alto(full_path, 'SXXP')



