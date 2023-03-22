import pandas as pd


def universe_up_perf():
    df = pd.read_excel('Universe up.xlsx', date_parser=['entry_date'],
                       sheet_name='MAIN')
    up_duration = 5
    up_perc = 0.0175

    status = 'No UP'
    df[f'up'] = 0

    day_list = [1, 2, 3, 4, 5, 10]

    for day in day_list:
        df[f'up {day}D'] = 0

    for index, row in df.iterrows():
        if status == 'No UP':
            current_mom_perc = row[f'alpha Universe']
            start_index = max(0, index - up_duration)
            min_mom_perc = df.loc[start_index:index, f'alpha Universe'].min()
            if current_mom_perc - min_mom_perc >= up_perc:
                status = 'UP'
                last_up_index = index
                df.loc[last_up_index, f'up'] = 10
                for day in day_list:
                    df.loc[last_up_index, f'up {day}D'] = \
                        df.loc[last_up_index+1:last_up_index+day, f'Alpha Alto Daily'].sum()
                wait_period = 0
        else:
            if wait_period < up_duration + 2:
                wait_period += 1
            else:
                status = 'No UP'
    print(1)


if __name__ == '__main__':
    universe_up_perf()
