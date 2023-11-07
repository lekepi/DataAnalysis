
import pandas as pd
from models import engine, config_class
import plotly.graph_objects as go
from plotly.subplots import make_subplots

if __name__ == '__main__':

    target_net = 0

    # AUM from alpha summary
    my_sql = """SELECT entry_date,sum(pnl_usd) as long_pnl, sum(mkt_value_usd) as long_notional FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE prod_type='Cash' and parent_fund_id=1 and quantity>0 
    and entry_date>='2019-04-01' and entry_date<'2023-10-13' group by entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    df['AUM'] = df['long_notional'].where(df['entry_date'].dt.to_period('M') != df['entry_date'].shift(1).dt.to_period('M')).ffill()

    if target_net != 0:
        df['long_notional'] = df['long_notional'] * (1+target_net/2)
        df['long_pnl'] = df['long_pnl'] * (1+target_net/2)

    my_sql = """SELECT entry_date,-sum(notional_usd) as short_notional, -sum(pnl_usd) as short_pnl FROM position_backtest 
    WHERE type='Universe' group by entry_date order by entry_date;"""
    df_bt = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    #merge df and df_bt
    df = pd.merge(df, df_bt, how='left', on='entry_date')

    if target_net != 0:
        df['short_notional'] = df['short_notional'] * (1-target_net/2)
        df['short_pnl'] = df['short_pnl'] * (1-target_net/2)

    df['Net Exposure'] = df['long_notional'] + df['short_notional']
    df['Net Exposure %'] = df['Net Exposure'] / df['AUM']
    df['Gross Exposure'] = df['long_notional'] + df['short_notional'].abs()
    df['Gross Exposure %'] = df['Gross Exposure'] / df['AUM']
    df['pnl'] = df['long_pnl'] + df['short_pnl']
    df['pnl %'] = df['pnl'] / df['AUM']

    df_month = df.groupby(df['entry_date'].dt.to_period('M')).agg({'AUM': 'first', 'pnl': 'sum'})
    # transform index to first date of that month
    df_month.index = df_month.index.to_timestamp('M')
    # in index replace day with 1
    df_month.index = df_month.index.map(lambda x: x.replace(day=1))

    df_month['pnl %'] = df_month['pnl'] / df_month['AUM']
    # Calculate cumulative performance
    df_month['pnl % cumul'] = (1 + df_month['pnl %']).cumprod() - 1

    df_month['pnl % fee'] = df_month['pnl %'] - 0.0075/12
    df_month['pnl % cumul with fees'] = (1 + df_month['pnl % fee']).cumprod() - 1

    # plotly dark theme with 'pnl % cum' and 'pnl % fee cum'
    fig = make_subplots()
    fig.add_trace(go.Scatter(x=df_month.index, y=df_month['pnl % cumul'], name='pnl % cumul'))
    fig.add_trace(go.Scatter(x=df_month.index, y=df_month['pnl % cumul with fees'], name='pnl % fee cumul'))
    # dark theme
    fig.update_layout(template='plotly_dark')
    # y axis in %
    fig.update_yaxes(tickformat=".2%")
    # add y legend: performance %
    fig.update_yaxes(title_text="Performance %")
    # title should be Performance with net target around 10%
    fig.update_layout(title_text=f"Performance with net exposure ~ {target_net*100}%", title_x=0.5)
    fig.show()

    # another figure with df 'Net Exposure %'
    fig = make_subplots()
    fig.add_trace(go.Scatter(x=df['entry_date'], y=df['Net Exposure %'], name='Net Exposure %'))
    # dark theme
    fig.update_layout(template='plotly_dark')
    # y axis in %
    fig.update_yaxes(tickformat=".2%")
    # add y legend: performance %
    fig.update_yaxes(title_text=f"Net Exposure %")
    # title should be Performance with net target around 10%
    fig.update_layout(title_text=f"Net Exposure - target {target_net*100}%", title_x=0.5)
    fig.show()

    # another figure with df 'gross Exposure %'
    fig = make_subplots()
    fig.add_trace(go.Scatter(x=df['entry_date'], y=df['Gross Exposure %'], name='Gross Exposure %'))
    # dark theme
    fig.update_layout(template='plotly_dark')
    # y axis in %
    fig.update_yaxes(tickformat=".2%")
    # add y legend: performance %
    fig.update_yaxes(title_text=f"Gross Exposure %")
    # title should be Performance with net target around 10%
    fig.update_layout(title_text=f"Gross Exposure % - target 200%", title_x=0.5)
    fig.show()

    # put df_month in excel
    df_month.to_excel('AEMN Analysis.xlsx')





    print(1)
