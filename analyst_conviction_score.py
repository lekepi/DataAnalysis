import pandas as pd
from models import engine


def get_conviction_score():
    my_sql = f"""SELECT last_date,alpha_point,current_size,ticker,target_size,T3.size,T3.ananda_sector_id,T4.name as ananda_sector,T2.first_name as analyst
                 FROM analyst_perf T1 JOIN user T2 on T1.user_id=T2.id LEFT JOIN Product T3 on T1.product_id=T3.id LEFT JOIN ananda_sector T4 on T3.ananda_sector_id=T4.id 
                 WHERE last_date>'2022-01-01' and  is_top_pick=0 and is_historic=0 and target_size<>0 order by last_date;"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['last_date'])
    df['alpha_1d'] = df['alpha_point'] / abs(df['current_size']) * 100

    # if target_size<0, then target_size is multiplied by 3
    df['new_target_size'] = df['target_size'].apply(lambda x: x * 3 if x < 0 else x)

    df['conviction'] = df['new_target_size'] / df['size'] * 3
    df['conviction'] = abs(df['conviction'].round(4))
    df['direction'] = df['target_size'].apply(lambda x: 'Long' if x > 0 else 'Short')

    # keep only when conviction in [-3, -2, -1, 1, 2, 3]
    df = df[df['conviction'] == df['conviction'].astype(int)]
    df = df[(df['conviction'] >= -3) & (df['conviction'] <= 3)]

    df['year'] = df['last_date'].dt.year

    pivot_df = df.pivot_table(
        index=['year', 'conviction'],  # Group by 'conviction'
        values='alpha_1d',  # The column to aggregate
        aggfunc=['mean', 'count']  # Aggregation functions
    ).reset_index()

    # put index back as column
    pivot_df.columns = pivot_df.columns.droplevel(1)

    yearly_data = pivot_df.groupby('year').agg({'count': 'sum'})
    # Calculate the weighted average of 'alpha'
    yearly_data['mean'] = pivot_df.groupby('year').apply(lambda x: (x['mean'] * x['count']).sum() / x['count'].sum())
    # Reset the index if you want 'year' to be a column instead of an index
    yearly_data = yearly_data.reset_index()
    # Add placeholder for 'conviction' since it's required in your original DataFrame
    yearly_data['conviction'] = None
    # Reorder columns to match the original DataFrame
    yearly_data = yearly_data[['year', 'conviction', 'mean', 'count']]

    conviction_data = pivot_df.groupby('conviction').agg({'count': 'sum'})
    # Calculate the weighted average of 'alpha'
    conviction_data['mean'] = pivot_df.groupby('conviction').apply(lambda x: (x['mean'] * x['count']).sum() / x['count'].sum())
    # Reset the index if you want 'year' to be a column instead of an index
    conviction_data = conviction_data.reset_index()
    # Add placeholder for 'conviction' since it's required in your original DataFrame
    conviction_data['year'] = None
    # Reorder columns to match the original DataFrame
    conviction_data = conviction_data[['year', 'conviction', 'mean', 'count']]

    # Append the new rows to the original DataFrame
    pivot_df = pd.concat([pivot_df, yearly_data, conviction_data], ignore_index=True)

    # export into \Excel
    pivot_df.to_excel(rf'Excel\conviction_score.xlsx', index=False, sheet_name='Conviction Score')


if __name__ == '__main__':
    get_conviction_score()
