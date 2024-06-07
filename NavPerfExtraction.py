import sys
from datetime import date, timedelta
import pandas as pd
import os
import numpy as np


file_list = ['Consolidated Financials_Ananda Long Term - Master_',
             'Consolidated Financials_Ananda Long Term_']


def get_nav_perf(my_date):

    print(f'Processing {my_date}')

    root_path = r'H:\NAV\DATA 2024-05-29'
    my_date_str1 = my_date.strftime("%Y%m%d")
    my_date_str2 = my_date.strftime("%m%d%Y")

    df_return = None

    full_path = os.path.join(root_path, my_date_str2)
    if os.path.exists(full_path):
        for file in file_list:
            if "Master" in file:
                file_type = 'Master'
            else:
                file_type = 'Feeder'

            file_name = f'{file}{my_date_str1}.XLSX'
            full_file_path = os.path.join(full_path, file_name)

            df = pd.read_excel(full_file_path, sheet_name='Summary Equity Schedule', skiprows=6)

            # replace nan with None
            df = df.replace({np.nan: None})

            df = df[['Class', 'PTD NET ROR', 'MTD NET ROR', 'QTD NET ROR', 'YTD NET ROR']]
            df['Date'] = my_date
            df['Type'] = file_type
            df['File Name'] = file_name

            # remove row when class is None
            df = df[df['Class'].notna()]

            # Put the last two columns first
            df = df[['Date', 'Type', 'Class', 'PTD NET ROR', 'MTD NET ROR', 'QTD NET ROR', 'YTD NET ROR', 'File Name']]

            if df_return is None:
                df_return = df
            else:
                df_return = pd.concat([df_return, df])

        return df_return
    else:
        return None


def get_nav_class_hedge(my_date):

    my_date_str2 = my_date.strftime("%m%d%Y")

    df_return = None
    root_path = r'H:\NAV\DATA 2024-05-29'
    full_path = os.path.join(root_path, my_date_str2)
    file_name = f'FX Hedge Report.xlsx'
    full_file_path = os.path.join(full_path, file_name)
    df = pd.read_excel(full_file_path, skiprows=3)
    # keep only 13 first row
    df = df.iloc[:13, :]
    df = df.replace({np.nan: None})
    df = df[df['Unnamed: 0'].notna()]
    df = df.drop(columns=['Unnamed: 1'])
    #add date
    df['Date'] = my_date
    # put date as first col - dynamically
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    return df


def get_nav_share_class(my_date):
    my_date_str2 = my_date.strftime("%m%d%Y")
    root_path = r'H:\NAV\DATA 2024-05-29'
    full_path = os.path.join(root_path, my_date_str2)
    file_name = f'Share Class Summary.xlsx'
    file_name2 = f'Share Class Summary_{my_date_str2}.xlsx'
    full_file_path = os.path.join(full_path, file_name)

    if not os.path.exists(full_file_path):
        full_file_path = os.path.join(full_path, file_name2)
        if not os.path.exists(full_file_path):
            return None

    df = pd.read_excel(full_file_path, skiprows=3)
    df['Date'] = my_date
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    return df


def get_nav_all_investor(my_date):
    df_investor = None
    my_date_str = my_date.strftime("%Y%m%d")
    my_date_str2 = my_date.strftime("%m%d%Y")
    root_path = r'H:\NAV\DATA 2024-05-29'
    full_path = os.path.join(root_path, my_date_str2)

    file_list = [f'Consolidated Financials_Ananda Euro Class_{my_date_str}.XLSX',
                 f'Consolidated Financials_Ananda Long Term GBP Class_{my_date_str}.XLSX',
                 f'Consolidated Financials_Ananda Long Term Opportunities_{my_date_str}.XLSX']

    for file_name in file_list:
        full_file_path = os.path.join(full_path, file_name)
        df = pd.read_excel(full_file_path, skiprows=6, sheet_name='Summary Equity Schedule')
        # remove ast 3 rows
        df = df.iloc[:-3, :]
        df['Date'] = my_date
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df = df[cols]

        if df_investor is None:
            df_investor = df
        else:
            df_investor = pd.concat([df_investor, df])

    return df_investor


if __name__ == '__main__':
    df_all = None
    df_class_hedge_all = None
    df_summary = None
    df_investor = None

    my_date = date(2024, 5, 1)
    last_date = date(2024, 5, 28)

    day = timedelta(days=1)
    while my_date <= last_date:
        week_num = my_date.weekday()
        if week_num < 5:  # ignore Weekend
            # get NAV Perf
            df = get_nav_perf(my_date)
            if df is not None:
                if df_all is None:
                    df_all = df
                else:
                    df_all = pd.concat([df_all, df])

            # get NAV Class Hedge
            df_class_hedge = get_nav_class_hedge(my_date)
            if df_class_hedge is not None:
                if df_class_hedge_all is None:
                    df_class_hedge_all = df_class_hedge
                else:
                    df_class_hedge_all = pd.concat([df_class_hedge_all, df_class_hedge])

            # get NAV Share Class
            df_share_class = get_nav_share_class(my_date)
            if df_share_class is not None:
                if df_summary is None:
                    df_summary = df_share_class
                else:
                    df_summary = pd.concat([df_summary, df_share_class])

            # get NAV All Investor
            df_inv = get_nav_all_investor(my_date)
            if df_inv is not None:
                if df_investor is None:
                    df_investor = df_inv
                else:
                    df_investor = pd.concat([df_investor, df_inv])

        my_date += day

    # save in Excel\ folder
    df_all.to_excel(r'H:\\NAV\DATA 2024-05-29\Nav Perf.xlsx', index=False)
    df_class_hedge_all.to_excel(r'H:\\NAV\DATA 2024-05-29\Nav Class Hedge.xlsx', index=False)
    df_summary.to_excel(r'H:\\NAV\DATA 2024-05-29\Nav Share Class.xlsx', index=False)
    df_investor.to_excel(r'H:\\NAV\DATA 2024-05-29\Nav All Investor.xlsx', index=False)
