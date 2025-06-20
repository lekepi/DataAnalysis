import pandas as pd
from models import engine
from datetime import date


def check_alpha_alto():

    my_sql = f"""SELECT entry_date, alpha_bp as alpha FROM alpha_summary WHERE parent_fund_id = 1 
          AND entry_date >= '2019-04-01' ORDER BY entry_date"""

    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'], index_col='entry_date')

    # Initialize all future alpha columns
    forward_days = [1, 2, 5, 10, 20]
    for d in forward_days:
        df[f'alpha_{d}d'] = None

    df['alpha_recent'] = None

    for i, (current_date, row) in enumerate(df.iterrows()):
        # Past 14 calendar days including current_date
        df_temp = df[(df.index <= current_date) & (df.index >= current_date - pd.DateOffset(days=14))]

        if not df_temp.empty:
            df_temp['alpha_cum'] = df_temp['alpha'].cumsum()
            alpha_min = df_temp['alpha_cum'].min()
            alpha_max = df_temp['alpha_cum'].max()
            alpha_last = df_temp['alpha_cum'].iloc[-1]

            if alpha_last - alpha_min >= 200 and alpha_last == alpha_max:
                alpha_value = round(alpha_last - alpha_min, 1)
                df.loc[current_date, 'alpha_recent'] = alpha_value

                # Fill forward-looking alpha columns
                for d in forward_days:
                    if i + d < len(df):
                        alpha_sum = df.iloc[i + 1:i + 1 + d]['alpha'].sum()
                        df.loc[current_date, f'alpha_{d}d'] = alpha_sum
        print(current_date)
    # Optional: Convert all to float
    df = df.astype({f'alpha_{d}d': float for d in forward_days} | {'alpha_recent': float})

    # Save to excel Excel\After_200bp_alpha
    df.to_excel('Excel/After_200bp_alpha_alto.xlsx', index=True)


if __name__ == '__main__':
    check_alpha_alto()
