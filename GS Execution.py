import pandas as pd
import win32com.client
import os
from datetime import datetime
from models import TradingGs, session
import numpy as np

search_dir = r'M:\CORE SPREADSHEETS\Trading\Email'
path_name = r'M:\CORE SPREADSHEETS\Trading\Excel'


def get_excel_from_msg():
    os.chdir(search_dir)
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    for file in files:
        if file.endswith(".msg"):
            index = file.find('European')
            file_name = file[:index-1]
            outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
            msg = outlook.OpenSharedItem(os.path.join(search_dir, file))
            att = msg.Attachments
            for i in att:
                i.SaveAsFile(os.path.join(path_name, f'{file_name}.xls'))


def save_file_db():

    trading_gs_list = []

    os.chdir(path_name)
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    for file in files:
        if "GS Weekly" in file:
            full_path = os.path.join(path_name, file)

            df_date = pd.read_excel(full_path, sheet_name='Report', header=7, usecols='A:A', nrows=2)
            my_date_str = df_date.columns[0].split(" -")[0]
            entry_date = datetime.strptime(my_date_str, '%d%b%y')
            df = pd.read_excel(full_path, sheet_name='Report', header=11)

            arrival_index = df[df['Unnamed: 0'] == 'ARRIVAL TIME'].index.values[0]
            df = df[arrival_index + 1:]
            df = df.replace({np.nan: None})

            for index, row in df.iterrows():
                entry_time = row['Unnamed: 0'].split(":")[0]
                if entry_time == " < 08":
                    entry_time = 7
                else:
                    entry_time = int(entry_time)
                orders = row['Orders']
                notional_usd = row['Notional $ Executed']
                alpha_close = row['Alpha to Close']
                open = row['Arrival Mid']
                close = row['Close']
                vwap = row['Full Day Cons VWAP']
                prev_close = row['Prev Close']
                arrival = row['Arrival Mid']

                new_trading_gs = TradingGs(entry_date=entry_date,
                                           entry_time=entry_time,
                                           orders=orders,
                                           notional_usd=notional_usd,
                                           alpha_close=alpha_close,
                                           open=open,
                                           close=close,
                                           vwap=vwap,
                                           prev_close=prev_close,
                                           arrival=arrival)

                trading_gs_list.append(new_trading_gs)
            print(file)
    if trading_gs_list:
        session.add_all(trading_gs_list)
        session.commit()


if __name__ == "__main__":
    save_file_db()
