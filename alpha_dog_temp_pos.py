import pandas as pd
from models import engine, session


def alpha_dog_analysis(past_alpha_period, future_alpha_period,percentile_num):
    my_sql = f"""SELECT entry_date,product_id,mkt_value_usd,past_alpha_{past_alpha_period},
    future_alpha_{future_alpha_period} FROM temp_position order by entry_date,past_alpha_{past_alpha_period};"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    max_date = df['entry_date'].max()

    if future_alpha_period == '1y':
        days = 366
    elif future_alpha_period == '6m':
        days = 183
    elif future_alpha_period == '3m':
        days = 92
    else:
        days = 0

    last_relevant_date = max_date - pd.Timedelta(days=days)
    df = df[df['entry_date'] <= last_relevant_date]

    df['percentile'] = df.groupby('entry_date')[f'past_alpha_{past_alpha_period}'].transform(
        lambda x: pd.qcut(x, q=percentile_num, labels=False, duplicates='drop'))

    df_avg = df.groupby('entry_date').agg({f'past_alpha_{past_alpha_period}': 'mean', f'future_alpha_{future_alpha_period}': 'mean'})
    # rename col 'past_avg' and 'future_avg'
    df_avg.columns = ['past_avg', 'future_avg']

    df_result = df.groupby(['entry_date', 'percentile'])[f'future_alpha_{future_alpha_period}'].mean().reset_index()
    # ranges = [(f'{i * 10}%-{(i + 1) * 10}%' if i < 9 else '90%-100%') for i in range(10)]
    ranges = [(f'{i * (100 // percentile_num)}%-{(i + 1) * (100 // percentile_num)}%' if i < percentile_num - 1
               else f'{(i * (100 // percentile_num))}%-100%') for i in range(percentile_num)]
    df_result['range'] = df_result['percentile'].map(lambda x: ranges[int(x)] if pd.notna(x) else None)

    # replace nan with None
    df_result = df_result.where(pd.notnull(df_result), None)

    # pivot table with range
    df_result = df_result.pivot(index='entry_date', columns='range', values=f'future_alpha_{future_alpha_period}')

    first_col = df_result.columns[0]
    last_col = df_result.columns[-1]
    df_result = df_result.join(df_avg)
    df_result[f'{first_col} Rel'] = df_result[first_col] - df_result['future_avg']
    df_result[f'{last_col} Rel'] = df_result[last_col] - df_result['future_avg']

    df_result[f'{first_col} Rel MA'] = df_result[f'{first_col} Rel'].rolling(window=15).mean()
    df_result[f'{last_col} Rel MA'] = df_result[f'{last_col} Rel'].rolling(window=15).mean()


    df_result.index = df_result.index.date

    # save to excel in excel\alpha dog folder
    df_result.to_excel(f'excel/alpha dog/alpha_dog_{percentile_num}_{past_alpha_period}_{future_alpha_period}.xlsx')
    print(f'{percentile_num}_{past_alpha_period}_{future_alpha_period}.xlsx')


if __name__ == "__main__":
    past_alpha_period = '1y'
    future_alpha_period = '1y'
    percentile_num = 10

    past_alpha_list = ['1y', '6m', '3m']
    future_alpha_list = ['1y', '6m', '3m']
    percentile_num_list = [20, 10, 5]

    for past_alpha_period in past_alpha_list:
        for future_alpha_period in future_alpha_list:
            for percentile_num in percentile_num_list:
                alpha_dog_analysis(past_alpha_period, future_alpha_period, percentile_num)
