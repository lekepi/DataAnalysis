import pandas as pd
from datetime import date, timedelta
from models import session, engine


def get_drop_stock(my_date, perc):

    # create empty dataframe with the column Date, Ticker, Return_1d, Prod_type, Continent
    df_result = pd.DataFrame(columns=['Date', 'Ticker', 'Return_1d', 'Relative_return', 'Prod_type', 'Continent'])


    my_sql = f"""SELECT ticker,return_1d,prod_type,continent FROM product_beta T1 JOIN product T2 
                 JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
                 on T1.product_id=T2.id where entry_date='{my_date}' and prod_type in ('Cash');"""
    df = pd.read_sql(my_sql, con=engine)

    # get index Return
    my_sql = f"""SELECT ticker,perf_1d,prod_type,continent FROM index_return T1 
                 JOIN product T2 JOIN exchange T3 on T2.exchange_id=T3.id 
                 JOIN country T4 on T3.country_id=T4.id on T1.product_id=T2.id where entry_date='{my_date}';"""
    df_index = pd.read_sql(my_sql, con=engine)

    # get SPX and SXXP return from df_index
    spx_return_1d = df_index[df_index['ticker'] == 'SPX Index']['perf_1d'].values[0]
    sxxp_return_1d = df_index[df_index['ticker'] == 'SXXP Index']['perf_1d'].values[0]

    for index, row in df.iterrows():
        prod_type = row['prod_type']
        continent = row['continent']
        ticker = row['ticker']
        return_1d = row['return_1d']
        if prod_type == 'Cash':
            if continent == 'AMER':
                index_return = spx_return_1d
            else:
                index_return = sxxp_return_1d

            relative_return = return_1d - index_return
            if relative_return < -perc:
                # add row to df_result
                new_row = {'Date': my_date, 'Ticker': ticker, 'Return_1d': return_1d, 'Relative_return': relative_return,
                           'Prod_type': prod_type, 'Continent': continent}
                new_row_df = pd.DataFrame([new_row])
                df_result = pd.concat([df_result, new_row_df], ignore_index=True)

    print(my_date)
    return df_result


if __name__ == '__main__':
    perc = 0.15
    df = pd.DataFrame(columns=['Date', 'Ticker', 'Return_1d', 'Relative_return', 'Prod_type', 'Continent'])
    my_date = date(2019, 4, 1)

    day = timedelta(days=1)
    while my_date < date.today():
        week_num = my_date.weekday()
        if week_num < 5:  # ignore Weekend
            df_result = get_drop_stock(my_date, perc)
            if df_result.empty is False:
                df = pd.concat([df, df_result], ignore_index=True)
        my_date += day

    # store in excel
    df.to_excel(rf'H:\TEMP\stock_drop_checker.xlsx', index=False)


