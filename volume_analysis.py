from datetime import date, timedelta
import pandas as pd
from models import engine
import os


def get_volume_data(my_date, parent_fund_id, exclude_ticker_list):
    my_sql = f"""SELECT trade_date,T2.ticker,continent,sum(notional_usd) as notional_usd,sum(quantity) as quantity 
    FROM trade T1 JOIN product T2 on T1.product_id=T2.id 
    JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
    WHERE parent_fund_id={parent_fund_id} and trade_date='{my_date}' and prod_type='Cash' and T2.ticker not in ({exclude_ticker_list})
    group by trade_date,T2.ticker,continent;"""
    df_trade = pd.read_sql(my_sql, con=engine)
    # remove when quantity is 0
    df_trade = df_trade[df_trade['quantity'] != 0]

    my_sql =f"""SELECT ticker,volume FROM product_market_data T1 JOIN product T2 on T1.product_id=T2.id 
    where entry_date='2019-04-01';"""
    df_volume = pd.read_sql(my_sql, con=engine)

    df = pd.merge(df_trade, df_volume, on='ticker', how='left')
    df['% of ADV'] = df['quantity'].abs() / df['volume']
    df['% of ADV'] = pd.to_numeric(df['% of ADV'], errors='coerce')
    df['volume_usd'] = df['notional_usd'].abs() / df['% of ADV']
    df['volume_usd'] = pd.to_numeric(df['volume_usd'], errors='coerce')


    if df['% of ADV'].notna().any():
        average_per_adv = df['% of ADV'].mean()
        median_per_adv = df['% of ADV'].median()
        worst_5_per_adv = df['% of ADV'].nlargest(5).mean()
        worst_10_per_adv = df['% of ADV'].nlargest(10).mean()
        worst_adv = df['% of ADV'].max()
        worst_name = df.loc[df['% of ADV'].idxmax(), 'ticker']

        # get the list of 10 highest df['% of ADV']
        illiquid_tickers = df.nlargest(10, '% of ADV')['ticker'].tolist()
    else:
        average_per_adv = median_per_adv = worst_5_per_adv = worst_10_per_adv = worst_adv = worst_name = None
        illiquid_tickers = None

    my_sql = f"""SELECT sum(mkt_value_usd) as long_usd FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    WHERE entry_date='{my_date}' and quantity>0 and prod_type='Cash' and parent_fund_id={parent_fund_id};"""
    long_usd = pd.read_sql(my_sql, con=engine).iloc[0, 0]
    total_buy_usd = df[df['notional_usd'] > 0]['notional_usd'].sum()
    total_sell_usd = df[df['notional_usd'] < 0]['notional_usd'].sum()
    total_trade_usd = df['notional_usd'].abs().sum()
    trade_vs_long_perc = total_trade_usd / long_usd if long_usd and long_usd != 0 else None

    dict_data = {
        'Date': my_date,
        'Long USD': long_usd,
        'Total Buy USD': total_buy_usd,
        'Total Sell USD': total_sell_usd,
        'Total Trade USD': total_trade_usd,
        'Trade vs Long %': trade_vs_long_perc,
        'Volume USD': df['volume_usd'].sum(),
        'Average % of ADV': average_per_adv,
        'Median % of ADV': median_per_adv,
        'Worst 5 % of ADV': worst_5_per_adv,
        'Worst 10 % of ADV': worst_10_per_adv,
        'Worst % of ADV': worst_adv,
        'Worst Ticker': worst_name
    }

    print(f"Date: {my_date} Done")

    return df, dict_data, illiquid_tickers


if __name__ == "__main__":

    parent_fund_id = 1
    # my_mode = "SpecificDay"
    my_date = date(2019, 4, 1)

    exclude_ticker_list = ['XNPS GY', 'ROR LN', 'RSW LN', 'RBT FP', 'WIZZ LN', 'SRP FP', 'FNOX SS', 'UNA NA',
                           'TKAMY US', 'KNYJY US', 'BSGR NA', 'FIE GY', 'APG US']
    # exclude_ticker_list = ['XNPS GY', 'UNA NA', 'TKAMY US', 'KNYJY US', 'APG US']

    exclude_ticker_list = "'" + "','".join(exclude_ticker_list) + "'"

    ticker_file = "volume_analysis_tickers.xlsx"
    if os.path.exists(ticker_file):
        df_ticker = pd.read_excel(ticker_file)
    else:
        my_sql = f"""SELECT T2.ticker FROM position T1 JOIN product T2 on T1.product_id=T2.id WHERE entry_date>'{my_date}'
         and prod_type='Cash' and parent_fund_id={parent_fund_id} and T2.ticker not in ({exclude_ticker_list}) group by ticker order by ticker;"""

        df_ticker = pd.read_sql(my_sql, con=engine)
        # save in excel
        df_ticker.to_excel("volume_analysis_tickers.xlsx", index=False)

    tickers = df_ticker['ticker'].tolist()
    df_result = pd.DataFrame(columns=tickers)

    df_illiquid = pd.DataFrame(index=tickers)
    df_illiquid['count'] = 0

    summary_data_list = []

    day = timedelta(days=1)
    while my_date < date.today():
        week_num = my_date.weekday()
        if week_num < 5:  # ignore Weekend
            df, dict_data, illiquid_tickers = get_volume_data(my_date, parent_fund_id, exclude_ticker_list)
            # Add dict_data to summary list
            summary_data_list.append(dict_data)

            # Extract % of ADV per ticker into a row (Series)
            row_series = df.set_index('ticker')['% of ADV']
            # Reindex to ensure all tickers are present; missing tickers get NaN
            row_series = row_series.reindex(tickers)
            df_result.loc[my_date] = row_series

            # Update illiquid counts
            if illiquid_tickers:  # only if the list is not empty
                for ticker in illiquid_tickers:
                    if ticker in df_illiquid.index:
                        df_illiquid.at[ticker, 'count'] += 1
        my_date += day

    df_summary = pd.DataFrame(summary_data_list)
    df_summary.set_index('Date', inplace=True)

    # Step 1: Identify the dates (index) where 'Trade vs Long %' > 0.1
    # TODO uncomment to remove gross up/down
    # bad_dates = df_summary[df_summary['Trade vs Long %'] > 0.1].index
    # df_summary = df_summary.drop(index=bad_dates, errors='ignore')
    # df_result = df_result.drop(index=bad_dates, errors='ignore')


    df_illiquid = df_illiquid[df_illiquid['count'] > 0].sort_values(by='count', ascending=False)

    # Save to Excel with two sheets
    with pd.ExcelWriter('volume_analysis_summary.xlsx') as writer:
        df_summary.to_excel(writer, sheet_name='Summary')
        df_result.to_excel(writer, sheet_name='Daily % of ADV')
        df_illiquid.to_excel(writer, sheet_name='Illiquid Tickers')


