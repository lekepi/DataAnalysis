import sqlite3
import pandas as pd



if __name__ == '__main__':
    # Connect to the SQLite database
    conn = sqlite3.connect('site.db')

    my_sql = "SELECT first_name,last_name,email, from user_activity T1 JOIN user T2  on T1.user_id=T2.id"

    df = pd.read_sql_query(my_sql, conn)
    conn.close()
    print(df)

    pass