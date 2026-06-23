import pandas as pd
from models import engine, session, TempAltoAlphaMixBm
from datetime import date, timedelta
import numpy as np


def get_new_bm_return(index_split=20):
    # get index return for amer and emea
    my_sql = f"""SELECT entry_date,T2.ticker,perf_1d FROM index_return T1 JOIN product T2 on T1.product_id=T2.id 
                WHERE entry_date>='2019-04-01' and ticker in ('SXXR Index', 'SPTR500N Index') order by entry_date;"""
    df_index = pd.read_sql(my_sql, con=engine)

    df_index_amer = df_index[df_index['ticker'] == 'SPTR500N Index'].copy()
    # remove ticker column from df_index_amer
    df_index_amer = df_index_amer.drop(columns='ticker')
    # rename column perf_1d to SPX
    df_index_amer = df_index_amer.rename(columns={'perf_1d': 'SPX'})

    df_index_emea = df_index[df_index['ticker'] == 'SXXR Index'].copy()
    # remove ticker column from df_index_emea
    df_index_emea = df_index_emea.drop(columns='ticker')
    # rename column perf_1d to SXXP
    df_index_emea = df_index_emea.rename(columns={'perf_1d': 'SXXP'})

    # get universe return for amer and emea
    my_sql = f"""SELECT entry_date,return_amer_y,return_emea_y FROM alpha_universe order by entry_date;"""
    df_universe = pd.read_sql(my_sql, con=engine)
    df_universe_amer = df_universe[['entry_date', 'return_amer_y']].copy()
    df_universe_emea = df_universe[['entry_date', 'return_emea_y']].copy()

    df_amer = pd.merge(df_index_amer, df_universe_amer, on='entry_date', how='inner')
    df_amer['bm_perf'] = df_amer['SPX'] * index_split/100 + df_amer['return_amer_y'] * (100 - index_split)/100

    df_emea = pd.merge(df_index_emea, df_universe_emea, on='entry_date', how='inner')
    df_emea['bm_perf'] = df_emea['SXXP'] * index_split/100 + df_emea['return_emea_y'] * (100 - index_split)/100

    # get daily BM return for amer and emea

    return df_amer, df_emea


if __name__ == '__main__':
    df_amer, df_emea = get_new_bm_return()

    my_sql = f"""SELECT entry_date,(long_amer+long_emea) as long_usd FROM alto_daily order by entry_date;"""
    df_long = pd.read_sql(my_sql, con=engine)

    my_date = date(2019, 4, 1)
    session.query(TempAltoAlphaMixBm).filter(TempAltoAlphaMixBm.entry_date >= my_date).delete()
    session.commit()

    day = timedelta(days=1)

    results = []

    while my_date < date.today():
        week_num = my_date.weekday()
        if week_num < 5:  # ignore Weekend

            amer_bm_perf = df_amer[df_amer['entry_date'] == my_date]['bm_perf'].values[0]
            emea_bm_perf = df_emea[df_emea['entry_date'] == my_date]['bm_perf'].values[0]
            long_usd = df_long[df_long['entry_date'] == my_date]['long_usd'].values[0]

            my_sql = f"""SELECT T1.product_id,T2.ticker,mkt_value_usd,continent,T5.beta,T5.return_1d 
            FROM position T1 JOIN product T2 on T1.product_id=T2.id 
            JOIN exchange T3 on T2.exchange_id=T3.id JOIN country T4 on T3.country_id=T4.id 
            JOIN product_beta T5 on T1.product_id=T5.product_id and T1.entry_date=T5.entry_date 
            WHERE parent_fund_id=1 and T1.entry_date='{my_date}' and prod_type='Cash';"""
            df_position = pd.read_sql(my_sql, con=engine)

            df_position['BM return'] = np.where(df_position['continent'] == 'AMER', amer_bm_perf, emea_bm_perf)
            df_position['alpha'] = df_position['return_1d'] - df_position['beta']*df_position['BM return']
            df_position['alpha_usd'] = df_position['alpha'] * df_position['mkt_value_usd']
            total_alpha_usd = df_position['alpha_usd'].sum()
            total_alpha_perf = total_alpha_usd / long_usd
            results.append({
                'entry_date': my_date,
                'total_alpha_usd': total_alpha_usd,
                'long_usd': long_usd,
                'total_alpha_perf': total_alpha_perf,
            })

            new_alpha_mix = TempAltoAlphaMixBm(entry_date=my_date,
                                               alpha_usd=total_alpha_usd,
                                               long_usd=long_usd,
                                               alpha=total_alpha_perf)

            session.add(new_alpha_mix)
            session.commit()

            print(f"{my_date},{total_alpha_perf:.4%}")
        my_date += day

        df_results = pd.DataFrame(results)
        # into Excel folder in excel
        df_results.to_excel('Excel/Alto_alpha_vs_Index_universe.xlsx', index=False)
