import pandas as pd
from models import engine
from datetime import date


if __name__ == '__main__':

    my_sql = f"""SELECT entry_date,product_id,ticker,return_1d FROM product_beta T1 JOIN product T2
                on T1.product_id=T2.id WHERE return_1d<-0.15 and entry_date>='2019-04-01';"""
    df_15 = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    my_sql = f"""SELECT ticker,first_name FROM analyst_universe T1 JOIN product T2
            on T1.product_id=T2.id JOIN user T3 on T1.user_id=T3.id order by product_id;"""
    df_universe = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    
    # remove duplicate ticker in df_universe
    df_universe = df_universe.drop_duplicates(subset=['ticker'], keep='first')

    # merge df_15 and df_universe both inner join by ticker

    df = pd.merge(df_15, df_universe, how='inner', on='ticker')

    my_sql ="""SELECT ticker, 1 as is_dog FROM analyst_selection T1 join product T2 on T1.product_id=T2.id WHERE is_dog=1;"""
    df_dog = pd.read_sql(my_sql, con=engine)
    # remove duplicate ticker in df_dog
    df_dog = df_dog.drop_duplicates(subset=['ticker'], keep='first')

    # merge df and df_dog by ticker
    df = pd.merge(df, df_dog, how='left', on='ticker')
    # is_dog at 0 in df if null
    df['is_dog'] = df['is_dog'].fillna(0)

    # remove when is_dog=1
    df = df[df['is_dog'] == 0]

    my_sql = """SELECT product_id,side,conviction,start_date,end_date FROM analyst_selection WHERE (conviction is not NULL and conviction<>0)
and (is_dog=0 or is_dog is NULL) and is_historic=0 and is_top_pick=0;"""
    df_selection = pd.read_sql(my_sql, con=engine, parse_dates=['start_date', 'end_date'])

    # when end_date is Null replace by date(2030,1,1)
    #convert start_date into date
    df_selection['start_date'] = df_selection['start_date'].dt.date
    df_selection['end_date'] = df_selection['end_date'].dt.date
    df_selection['end_date'] = df_selection['end_date'].fillna(date(2030, 1, 1))

    # convert df entry_time into date
    df['entry_date'] = df['entry_date'].dt.date
    # loop in df row:
    # if entry_date is between start_date and end_date, then add conviction to df
    for index, row in df.iterrows():
        # find rows with product_id and between start_date and end_date
        df_temp = df_selection[(df_selection['product_id'] == row['product_id']) & (df_selection['start_date'] < row['entry_date']) & (df_selection['end_date'] >= row['entry_date'])]
        if len(df_temp) > 0:
            df.loc[index, 'side'] = df_temp['side'].values[0]
            df.loc[index, 'conviction'] = df_temp['conviction'].values[0]

    print(1)