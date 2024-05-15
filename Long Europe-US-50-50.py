from models import engine
from datetime import date, timedelta
import pandas as pd


def get_portfolio_perf(reset):
    my_sql = f"""SELECT entry_date,product_id,mkt_value_usd,perf_1d,quantity,currency_id,T4.continent FROM position T1 JOIN product T2 on T1.product_id=T2.id 
    JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id
     WHERE parent_fund_id=1 and entry_date>='2019-04-01' and prod_type='Cash' and quantity>0 order by entry_date,product_id;"""
    df_pos = pd.read_sql(my_sql, con=engine)

    # get all currency_id in a list
    currency_id_list = df_pos['currency_id'].unique().tolist()
    # get currency_id_list in one string separated by comma
    currency_id_list_str = ",".join([str(x) for x in currency_id_list])

    # my_sql = f"""SELECT entry_date,currency_id,rate FROM currency_history WHERE entry_date>='2019-03-31' order by entry_date,currency_id"""
    my_sql = f"""SELECT entry_date,currency_id,rate FROM currency_history WHERE entry_date>='2019-03-31'
    and currency_id in ({currency_id_list_str}) order by currency_id,entry_date"""
    df_fx = pd.read_sql(my_sql, con=engine)

    currency1_df = df_fx[df_fx['currency_id'] == 1]

    # Merge the filtered DataFrame with the original DataFrame on 'entry_date'
    df_cncy = df_fx.merge(currency1_df[['entry_date', 'rate']], on='entry_date', suffixes=('', '_currency1'))

    # Divide the rate by the rate of currency_id=1
    df_cncy['eur_rate'] = df_cncy['rate'] /df_cncy['rate_currency1']
    # keep only the column entry_date, currency_id, adjusted_rate
    df_cncy = df_cncy[['entry_date', 'currency_id', 'eur_rate']]

    # sort by currency_id and entry_date
    df_cncy = df_cncy.sort_values(by=['currency_id', 'entry_date'])
    df_cncy['last_eur_rate'] = df_cncy.groupby('currency_id')['eur_rate'].shift(1)

    # inverse cncy
    df_cncy['last_eur_rate'] = 1 / df_cncy['last_eur_rate']
    df_cncy['eur_rate'] = 1 / df_cncy['eur_rate']

    # merge df_pos and df_cncy
    df_pos = df_pos.merge(df_cncy, on=['entry_date', 'currency_id'], how='left')

    df_pos['fx_change'] = df_pos['eur_rate'] / df_pos['last_eur_rate'] - 1
    df_pos['mkt_value_eur'] = df_pos['mkt_value_usd'] * df_pos['last_eur_rate']
    df_pos['mkt_value_eur_end'] = df_pos['mkt_value_eur'] * (1 + df_pos['fx_change']) * (1 + df_pos['perf_1d'])

    # move eur_rate after the last_eur_rate
    df_pos = df_pos[['entry_date', 'product_id', 'mkt_value_usd', 'perf_1d', 'quantity', 'currency_id', 'continent',
                     'last_eur_rate', 'eur_rate',  'fx_change', 'mkt_value_eur', 'mkt_value_eur_end']]

    # # df is all unique entry_date
    date_list = df_pos['entry_date'].unique()
    date_list = [date(2019, 3, 31)] + date_list.tolist()

    # create df with index=date_list and columns=['perf_amer', 'perf_eur']
    df = pd.DataFrame(index=date_list, columns=['perf_amer', 'perf_eur', 'amount_amer', 'amount_eur', 'perf_total'])

    df['perf_amer'] = 0
    df['perf_eur'] = 0

    count = 0

    for index, row in df.iterrows():
        my_date = index

        if my_date != date(2019, 3, 31):
            df_pos_temp_amer = df_pos[(df_pos['entry_date'] == my_date) & (df_pos['continent'] == 'AMER')]
            total_start_eur = df_pos_temp_amer['mkt_value_eur'].sum()
            total_end_eur = df_pos_temp_amer['mkt_value_eur_end'].sum()
            perf_amer = (total_end_eur / total_start_eur - 1) * 100

            df_pos_temp_emea = df_pos[(df_pos['entry_date'] == my_date) & (df_pos['continent'] != 'AMER')]
            total_start_eur = df_pos_temp_emea['mkt_value_eur'].sum()
            total_end_eur = df_pos_temp_emea['mkt_value_eur_end'].sum()
            perf_emea = (total_end_eur / total_start_eur - 1) * 100

            df.loc[my_date, 'perf_amer'] = perf_amer
            df.loc[my_date, 'perf_eur'] = perf_emea

            new_amount_amer = previous_amount_amer * (1 + perf_amer / 100)
            new_amount_emea = previous_amount_emea * (1 + perf_emea / 100)

            df.loc[my_date, 'amount_amer'] = new_amount_amer
            df.loc[my_date, 'amount_eur'] = new_amount_emea

            df.loc[my_date, 'perf_total'] = (new_amount_amer + new_amount_emea) / \
                                            (previous_amount_amer + previous_amount_emea) - 1

            count += 1

            if count == reset:
                count = 0
                previous_amount_amer = 100
                previous_amount_emea = 100
            else:
                previous_amount_amer = new_amount_amer
                previous_amount_emea = new_amount_emea

        else:
            df.loc[my_date, 'amount_amer'] = 100
            df.loc[my_date, 'amount_eur'] = 100

            previous_amount_amer = 100
            previous_amount_emea = 100
        print(my_date)

    df['perf_cumul'] = (1 + df['perf_total']).cumprod() - 1

    df_monthly = df['perf_total']
    # convert index as string
    df_monthly.index = df_monthly.index.astype(str)
    df_monthly.index = df_monthly.index.str[:7]

    df_monthly = df_monthly.groupby(df_monthly.index).apply(lambda x: (1 + x).prod() - 1)
    # df_monthly from serie to df
    df_monthly = df_monthly.to_frame()
    df_monthly.columns = ['perf_total']
    df_monthly['perf_cumul'] = (1 + df_monthly['perf_total']).cumprod() - 1




    # df into excel in Excel folder
    df.to_excel(f'Excel/portfolio_perf-50-50-reset{reset}.xlsx')
    df_monthly.to_excel(f'Excel/portfolio_perf-50-50-reset{reset}-monthly.xlsx')


if __name__ == '__main__':
    get_portfolio_perf(reset=20)
