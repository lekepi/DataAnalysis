import pandas as pd
from models import engine


def get_analyst_size():

    # get sql that is the list of ticker speparated my comma
    ticker_str = ','.join([f"'{x}'" for x in ticker_list])

    my_sql = f"""SELECT last_date,T2.ticker,current_size FROM analyst_perf T1
    JOIN product T2 on T1.product_id=T2.id
    WHERE is_historic=0 and is_top_pick=0 and T2.ticker in ({ticker_str})
    order by last_date;"""

    df = pd.read_sql(my_sql, con=engine)

    # pivot by ticker
    df = df.pivot(index='last_date', columns='ticker', values='current_size')
    df.fillna(0, inplace=True)
    # add total column
    df['Total'] = df.sum(axis=1)

    df.to_excel(rf'Excel\analyst_size.xlsx', index=True, sheet_name='Analyst Size')


if __name__ == '__main__':

    ticker_list = ['WEIR LN', 'EPIA SS', 'SAND SS']
    get_analyst_size()
