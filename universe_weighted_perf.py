import pandas as pd
import numpy as np
from datetime import date


def get_universe_perf():

    df_final = pd.DataFrame()

    file_full_path = 'Excel/Universe Analysis 15-05-2024.xlsx'
    df = pd.read_excel(file_full_path)
    df = df.drop(columns=['MK 19', 'MK 24'])
    region_list = ['SPX', 'SXXP']

    for region in region_list:
        df_region = df[df['Index'] == region]
        df_region = df_region.drop(columns=['Index'])
        df_region = df_region.set_index('Ticker').T
        df_region.columns = df_region.columns.str.replace(' Equity', '')
        for col in df_region.columns:
            df_region.rename(columns={col: str(col) + " " + str(int(df_region.loc['classification'][col]))}, inplace=True)
        df_region = df_region.drop(index='classification')
        # get pct_change
        df_region = df_region.pct_change()
        # remove first row
        df_region = df_region.iloc[1:]
        df_region['average'] = None
        df_region['mkt_cap_avg'] = None
        df_region['Noreb'] = None
        df_region.index = pd.to_datetime(df_region.index).date

        # replace nan with None
        df_region = df_region.replace({np.nan: None})
        df_region['average'] = df_region.mean(axis=1)
        if df_final.empty:
            df_final.index = df_region.index
        df_final[region + " Equal Weighted"] = df_region['average']

        for index, row in df_region.iterrows():
            total = 0
            multi = 0
            for col in df_region.columns[:-3]:
                value = row[col]
                if value is not None:
                    current_multi = int(col[-1])
                    total += value * current_multi
                    multi += current_multi
            df_region.at[index, 'mkt_cap_avg'] = total / multi

        df_final[region + " Market Cap Weighted"] = df_region['mkt_cap_avg']

        # no rebalancing = cumulative

        index_list = df_region.index.tolist()
        index_list.insert(0, '2019-03-31')
        columns_list = df_region.columns.tolist()[0:-3]
        df_region_Noreb = pd.DataFrame(index=index_list, columns=columns_list)
        df_region_Noreb = df_region_Noreb.applymap(lambda x: None)
        # convert index to date
        df_region_Noreb.index = pd.to_datetime(df_region_Noreb.index).date

        # convert index into date, not datetime
        df_region_Noreb['multi'] = None
        df_region_Noreb['perf'] = None

        previous_index = date(2019, 3, 31)
        previous_entry = 100
        previous_total = 0

        for index, row in df_region.iterrows():
            total = 0
            multi = 0
            for col in df_region.columns[:-3]:
                value = row[col]
                stock_weight = int(col[-1])
                if value is not None:
                    if df_region_Noreb.loc[previous_index, col] is None:
                        df_region_Noreb.loc[previous_index, col] = previous_entry * stock_weight
                        previous_total += df_region_Noreb.loc[previous_index, col]
                    df_region_Noreb.loc[index, col] = df_region_Noreb.loc[previous_index, col] * (1 + value)

                    total += df_region_Noreb.loc[index, col]
                    multi += stock_weight
                else:   # the stock has no return anymore
                    if df_region_Noreb.loc[previous_index, col]:  # the stock had a return
                        previous_total -= df_region_Noreb.loc[previous_index, col]
            
            df_region_Noreb.loc[index, 'total'] = total
            df_region_Noreb.loc[index, 'previous_total'] = previous_total
            df_region_Noreb.loc[index, 'weight'] = multi
            df_region_Noreb.loc[index, 'perf'] = total / previous_total - 1
            df_region.loc[index, 'Noreb'] = total / previous_total - 1

            previous_entry = total / multi
            previous_index = index
            previous_total = total

        # save into excel/
        df_region_Noreb.to_excel('Excel/Universe ' + region + ' Noreb.xlsx')
        df_region.to_excel('Excel/Universe ' + region + '.xlsx')
        df_final[region + " Noreb"] = df_region['Noreb']

    df_final['Equal Weighted'] = 2 / 3 * df_final['SXXP Equal Weighted'] + 1 / 3 * df_final['SPX Equal Weighted']
    df_final['Market Cap Weighted'] = 2 / 3 * df_final['SXXP Market Cap Weighted'] + 1 / 3 * df_final['SPX Market Cap Weighted']
    df_final['Noreb'] = 2 / 3 * df_final['SXXP Noreb'] + 1 / 3 * df_final['SPX Noreb']
    # save into excel/
    df_final.to_excel('Excel/Universe Weighted Perf.xlsx')
    pass


if __name__ == '__main__':
    get_universe_perf()
