from models import engine
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import plotly


def get_nav_statement_graph(field_name):
    my_sql = f"""SELECT entry_date,data_name,{field_name} FROM nav_account_statement WHERE entry_date>='2024-05-01' 
    and status='Daily' and active=1;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])
    # pivot table
    df = df.pivot(index='entry_date', columns='data_name', values=field_name)
    # divide value by 100
    df = df / 100

    # remove 'RETURN USD GLD CLASS L'
    df = df.drop(columns=['RETURN USD GLD CLASS L'])

    # if col contains 'CLASS A' or 'CLASS F' multiply value by 2
    for col in df.columns:
        if 'CLASS A' in col or 'CLASS F' in col:
            df[col] = df[col] * 2

    fig = px.line(df, x=df.index, y=df.columns, template='plotly_dark')
    fig.layout.yaxis.tickformat = ',.2%'

    fig.update_layout(title=f'NAV Statement {field_name}', xaxis_title='Date', yaxis_title='NAV')
    fig.show()


if __name__ == '__main__':
    get_nav_statement_graph('data_daily')
    get_nav_statement_graph('data_mtd')
    get_nav_statement_graph('data_qtd')
    get_nav_statement_graph('data_ytd')
