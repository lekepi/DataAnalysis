import pandas as pd
from models import engine


if __name__ == '__main__':
    my_sql = f"""SELECT entry_date,account_value-margin_requirement as unenc FROM anandaprod.margin WHERE entry_date>'2024-06-20' order by entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # sum by entry_date
    df_sum = df.groupby('entry_date').sum().reset_index()

    # get minimum entry_date
    min_date = df_sum['entry_date'].min()

    my_sql = f"""SELECT entry_date,amount as aum FROM anandaprod.aum WHERE fund_id=4 and entry_date>='2024-05-20' and type='Fund';"""
    df_aum = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # replace first entry_date in df_aum by min_date
    df_aum.loc[df_aum['entry_date'] == df_aum['entry_date'].min(), 'entry_date'] = min_date

    # merge df_sum and df_aum on entry_date
    df_merged = pd.merge(df_sum, df_aum, on='entry_date', how='left')

    # fill with previous values
    df_merged['aum'] = df_merged['aum'].fillna(method='ffill')
    # calculate unencumbered cash %
    df_merged['unenc_perc'] = df_merged['unenc'] / df_merged['aum'] * 100
    # remove when %<5
    df_merged = df_merged[df_merged['unenc_perc'] >=10]
    min_perc = df_merged['unenc_perc'].min()

    print(f"Minimum unencumbered cash %: {min_perc:.2f}%")


    pass