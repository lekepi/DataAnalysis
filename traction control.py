import pandas as pd
from models import session, engine
from datetime import date
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from docx import Document
from docx.shared import Inches


def get_df_alpha(ticker, my_date):
    window = 50
    min_trend = 50
    my_sql = f"""SELECT entry_date,alpha FROM product_beta T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE ticker='{ticker}' and entry_date>'2019-01-01' and entry_date<'{my_date}' order by entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
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

    # df.to_excel('excel\\trend analysis.xlsx', index=True)

    fig = go.Figure()


    # Add trace for alpha with its own y-axis
    fig.add_trace(go.Scatter(x=df['entry_date'], y=df['alpha'], mode='lines', name='Alpha', yaxis='y'))

    # Add trace for trend_value with its own y-axis
    # fig.add_trace(go.Scatter(x=df['entry_date'], y=df['Trend_value'], mode='lines', name='Trend Value', yaxis='y2'))

    # Update layout to add secondary y-axis
    fig.update_layout(
        # template='plotly_dark',
        yaxis=dict(title='Trend Value'),
        # yaxis2=dict(title='Alpha', overlaying='y', side='right', showgrid=False),
        title='Trend Value and Alpha Over Time',
        xaxis=dict(title='Entry Date'),
        title_text=ticker
    )
    # fig.show()
    # save fig as image
    fig.write_image('plot.png', width=800, height=580)

    doc_path = r'H:\Louis Requests\Traction Control 2024-03-19.docx'
    doc = Document(doc_path)
    doc.add_picture('plot.png')
    doc.save(doc_path)
    print(ticker)


if __name__ == '__main__':
    today = date.today()
    my_sql = f"""SELECT T2.ticker FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE parent_fund_id=1 
    and prod_type='Cash' and mkt_value_usd>0  and entry_date='{today}' order by mkt_value_usd desc;"""
    df_ticker = pd.read_sql(my_sql, con=engine)
    ticker_list = df_ticker['ticker'].tolist()

    ticker_list = ['ADYEN NA', 'DB1 GY', 'CPR IM', 'WEIR LN', 'RSW LN', 'CNA LN', 'CNI US', 'LR FP', 'ITRK LN', 'BDEV LN', 'KSP ID',
                   'BP/ LN', 'DOKA SW', 'FGR FP', 'ROP US', 'ADBE US', 'IWG LN', 'TOI CN', 'BABA US', 'PSN LN', 'CRM US',
                   'EW US', 'ALLFG NA', 'GOLD US', 'WDAY US', 'AEM US', 'MASI US']

    # order ticker_list by alphabetic order
    ticker_list.sort()

    for ticker in ticker_list:
        df = get_df_alpha(ticker, today)


    pass