
import pandas as pd
from models import engine


if __name__ == '__main__':
    my_sql = "SELECT entry_date,data_name,data_mtd FROM nav_account_statement WHERE active=1 and status='MonthEnd';"
    df = pd.read_sql(my_sql, con=engine)

    # pivot
    df = df.pivot(index='entry_date', columns='data_name', values='data_mtd')

    pass
