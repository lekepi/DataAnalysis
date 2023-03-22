import pandas as pd


def get_momemtum_shock_perf():
    df = pd.read_excel('momentum_vs_alto.xlsx', date_parser=['entry_date'],
                       sheet_name='alpha_alto')
    shock_drop_duration = 10
    shock_drop_perc = 10
    region_list = ['amer', 'emea']

    for i in range(0, 2):
        region = region_list[i]
        status = 'No Shock'
        df[f'shock {region}'] = 0

        day_list = [1, 2, 3, 4, 5, 10]

        for day in day_list:
            df[f'shock {region} {day}D'] = 0

        for index, row in df.iterrows():
            if status == 'No Shock':
                current_mom_perc = row[f'momentum_{region}']
                start_index = max(0, index - shock_drop_duration)
                max_mom_perc = df.loc[start_index:index, f'momentum_{region}'].max()
                if max_mom_perc - current_mom_perc >= shock_drop_perc:
                    status = 'Shock'
                    last_shock_index = index
                    df.loc[last_shock_index, f'shock {region}'] = 10
                    for day in day_list:
                        df.loc[last_shock_index, f'shock {region} {day}D'] = \
                            df.loc[last_shock_index+1:last_shock_index+day, f'alpha_{region}'].sum()
                    wait_period = 0
            else:
                if wait_period < shock_drop_perc + 2:
                    wait_period += 1
                else:
                    status = 'No Shock'
    print(1)


if __name__ == '__main__':
    get_momemtum_shock_perf()
