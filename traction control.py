import pandas
from models import session, engine
import numpy as np


def get_df_alpha(ticker):
    window = 50
    min_trend = 50
    my_sql = f"""SELECT entry_date,alpha FROM anandaprod.product_beta T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE ticker='{ticker}' and entry_date>'2019-01-01' order by entry_date;"""
    df = pandas.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    df['alpha_sum'] = df['alpha'].cumsum()
    df['alpha_sum'] = df['alpha_sum'].fillna(0)
    # remove the alpha column
    df = df.drop(columns=['alpha'])
    df = df.rename(columns={'alpha_sum': 'alpha'})
    #add column called count
    df['count'] = range(1, len(df) + 1)
    half_window = int(window / 2)
    df['alpha MA'] = df['alpha'].rolling(window=window+1, min_periods=half_window, center=True).mean()
    df['slope'] = None

    for i in range(len(df)):
        if i < half_window or i >= len(df) - half_window:
            continue
        alpha_subset = df['alpha'].iloc[i - half_window:i + half_window]
        count_subset = df['count'].iloc[i - half_window:i + half_window]
        coeffs = np.polyfit(count_subset, alpha_subset, 1)
        df.at[df.index[i], 'slope'] = coeffs[0]

    df['slope MA'] = df['slope'].rolling(window=21, min_periods=10, center=True).mean()

    slope_sign = "+"
    last_change_len = 0
    last_index = 0
    df['Trend_value'] = 0
    df['Trend'] = None
    df['Check'] = None
    for index, row in df.iterrows():
        slope = row['slope MA']
        if slope:

            try:
                if slope > 0 and slope_sign == "+":
                    last_change_len += 1
                elif slope < 0 and slope_sign == "-":
                    last_change_len += 1
                elif slope > 0 and slope_sign == "-":
                    if last_change_len > min_trend:
                        df.loc[index, 'Check'] = "Change"
                        df.loc[index-last_change_len-1:index-1, 'Trend'] = "Down"
                        df.loc[index - last_change_len - 1:index - 1, 'Trend_value'] = -1
                        df.loc[last_index, 'Trend'] = "Trend Change"
                        last_index = index
                    slope_sign = "+"
                    last_change_len = 0
                elif slope < 0 and slope_sign == "+":
                    if last_change_len > min_trend:
                        df.loc[index, 'Check'] = "Change"
                        df.loc[index-last_change_len-1:index-1, 'Trend'] = "Up"
                        df.loc[index - last_change_len - 1:index - 1, 'Trend_value'] = 1
                        df.loc[last_index, 'Trend'] = "Trend Change"
                        last_change_len = 0
                        last_index = index
                    slope_sign = "-"
                    last_change_len = 0
            except(Exception):
                print(Exception)
                pass
    df.to_excel('excel\\trend analysis.xlsx', index=True)


if __name__ == '__main__':
    df = get_df_alpha('META US')


    pass