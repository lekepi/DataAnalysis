from datetime import date, timedelta
from models import engine
import pandas as pd
import os


def get_alto_vs_analyst(my_date):
    # =======================
    # Load Coverage Universe
    # =======================
    my_sql = f"""
    SELECT T2.ticker, T3.first_name FROM analyst_universe T1 JOIN product T2 ON T1.product_id = T2.id
    JOIN user T3 ON T1.user_id = T3.id WHERE start_date <= '{my_date}' AND (end_date >= '{my_date}' 
    OR end_date IS NULL) and priority=1 ORDER BY first_name, ticker;"""
    df_coverage = pd.read_sql(my_sql, con=engine)

    # =======================
    # Load ALTO Positions
    # =======================
    my_sql = f"""SELECT entry_date, T2.ticker, mkt_value_usd, alpha_usd FROM position T1 JOIN product T2 
    ON T1.product_id = T2.id WHERE entry_date = '{my_date}' AND parent_fund_id = 1 AND prod_type = 'Cash';"""
    df_alto = pd.read_sql(my_sql, con=engine)

    # Join and classify positions
    df = df_alto.merge(df_coverage, on='ticker', how='inner')
    df['side'] = df['mkt_value_usd'].apply(lambda x: 'Long' if x > 0 else 'Short')

    # Group by analyst and side
    df_alto_grouped = df.groupby(['entry_date', 'first_name', 'side'], as_index=False).agg({
        'mkt_value_usd': 'sum',
        'alpha_usd': 'sum'
    })

    # Pivot for analyst-level columns
    df_analyst_pivot = df_alto_grouped.pivot_table(
        index='entry_date',
        columns=['first_name', 'side'],
        values=['mkt_value_usd', 'alpha_usd'],
        aggfunc='sum',
        fill_value=0
    )
    df_analyst_pivot.columns = [
        f"{analyst}_{side.lower()}" if metric == 'mkt_value_usd' else f"{analyst}_alpha_{side.lower()}"
        for metric, analyst, side in df_analyst_pivot.columns
    ]

    # Total-level columns
    total_df = df_alto_grouped.groupby(['entry_date', 'side'], as_index=False).agg({
        'mkt_value_usd': 'sum',
        'alpha_usd': 'sum'
    })
    df_total_pivot = total_df.pivot(index='entry_date', columns='side', values=['mkt_value_usd', 'alpha_usd'])
    df_total_pivot.columns = [
        f"total_{side.lower()}" if metric == 'mkt_value_usd' else f"total_alpha_{side.lower()}"
        for metric, side in df_total_pivot.columns
    ]

    # Final ALTO DataFrame
    df_alto_final = pd.concat([df_total_pivot, df_analyst_pivot], axis=1)

    # =======================
    # Load Analyst Picks
    # =======================
    my_sql = f"""SELECT T2.id,T1.last_date AS entry_date,T2.ticker,T3.first_name,alpha_point * 10000 AS alpha_usd,
        current_size * 1000000 AS notional_usd FROM analyst_perf T1 JOIN product T2 ON T1.product_id = T2.id
    JOIN user T3 ON T3.id = T1.user_id WHERE T1.last_date = '{my_date}' AND priority = 1 AND is_top_pick = 0
      AND is_historic = 0;"""
    df_analyst_perf = pd.read_sql(my_sql, con=engine)
    df_analyst_perf['side'] = df_analyst_perf['notional_usd'].apply(lambda x: 'Long' if x > 0 else 'Short')

    # Group by analyst and side
    df_analyst_perf_grouped = df_analyst_perf.groupby(['entry_date', 'first_name', 'side'], as_index=False).agg({
        'notional_usd': 'sum',
        'alpha_usd': 'sum'
    })

    # Pivot for analyst-level picks
    df_analyst_pivot = df_analyst_perf_grouped.pivot_table(
        index='entry_date',
        columns=['first_name', 'side'],
        values=['notional_usd', 'alpha_usd'],
        aggfunc='sum',
        fill_value=0
    )
    df_analyst_pivot.columns = [
        f"{analyst}_{side.lower()}" if metric == 'notional_usd' else f"{analyst}_alpha_{side.lower()}"
        for metric, analyst, side in df_analyst_pivot.columns
    ]

    # Total-level picks
    total_df = df_analyst_perf_grouped.groupby(['entry_date', 'side'], as_index=False).agg({
        'notional_usd': 'sum',
        'alpha_usd': 'sum'
    })
    df_total_pivot = total_df.pivot(index='entry_date', columns='side', values=['notional_usd', 'alpha_usd'])
    df_total_pivot.columns = [
        f"total_{side.lower()}" if metric == 'notional_usd' else f"total_alpha_{side.lower()}"
        for metric, side in df_total_pivot.columns
    ]

    # Final Analyst Picks DataFrame
    df_analyst_final = pd.concat([df_total_pivot, df_analyst_pivot], axis=1)
    print(my_date)
    return df_alto_final, df_analyst_final


if __name__ == "__main__":
    #my_mode = "Today"
    # my_mode = "SpecificDay"
    my_mode = "RangeDays"

    if my_mode == "Today":
        my_date = date.today()
        day = timedelta(days=1)
        day3 = timedelta(days=3)
        if my_date.weekday() == 0:
            my_date -= day3
        else:
            my_date -= day
        get_alto_vs_analyst(my_date)
    elif my_mode == "SpecificDay":
        my_date = date(2026, 1, 15)
        get_alto_vs_analyst(my_date)
    else:  # Range / Loop
        my_date = date(2022, 1, 3)
        day = timedelta(days=1)

        alto_dfs = []
        analyst_dfs = []

        while my_date < date.today():
            if my_date.weekday() < 5:  # skip weekends
                try:
                    df_alto_final, df_analyst_final = get_alto_vs_analyst(my_date)

                    df_alto_final.index = pd.to_datetime(df_alto_final.index)
                    df_analyst_final.index = pd.to_datetime(df_analyst_final.index)

                    alto_dfs.append(df_alto_final)
                    analyst_dfs.append(df_analyst_final)

                except Exception as e:
                    print(f"Error processing {my_date}: {e}")

            my_date += day

        # Concatenate all results into final DataFrames
        df_alto_all = pd.concat(alto_dfs)
        df_analyst_all = pd.concat(analyst_dfs)

        # Optional: sort by date
        df_alto_all.sort_index(inplace=True)
        df_analyst_all.sort_index(inplace=True)

        # store both df as excel in Excel folder

        output_folder = "Alto vs Analyst"
        os.makedirs(output_folder, exist_ok=True)

        # Save ALTO positions
        alto_output_path = os.path.join(output_folder, "alto_positions.xlsx")
        df_alto_all.to_excel(alto_output_path)

        # Save Analyst picks
        analyst_output_path = os.path.join(output_folder, "analyst_picks.xlsx")
        df_analyst_all.to_excel(analyst_output_path)
