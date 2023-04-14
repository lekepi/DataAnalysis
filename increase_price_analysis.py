import pandas as pd
from openpyxl import Workbook, load_workbook
from utils import get_df_des, append_excel


if __name__ == '__main__':
    wb = Workbook()
    ws = wb.active
    ws.title = 'Data'
    wb.save('H:\Python Output\Alto Only\Increase Price Result.xlsx')

    df = pd.read_csv("H:\Python Output\Alto Only\increase_price.csv")
    df['Entry_date'] = pd.to_datetime(df['Entry_date'], format='%d/%m/%Y')

    # all cases
    df_des = get_df_des(df)
    append_excel(df_des, ["All Cases"], ws)

    #alto
    df_excel = df[df['Alto'] == 'B']
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Alto"], ws)

    df_excel = df[df['Alto'] == 'S']
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Short Alto"], ws)

    # before 2021
    df_excel = df[(df['Entry_date'] < '2021') & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Before 2021"], ws)

    # after 2021
    df_excel = df[(df['Entry_date'] >= '2021') & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long After 2021"], ws)

    # cluster
    df_excel = df[(df['Cluster_Num'] < 5) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Cluster<5"], ws)

    df_excel = df[(df['Cluster_Num'] >= 5) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Cluster 5-10"], ws)

    # Moving Average
    df_excel = df[(df['Px_Above_MA']) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Above MA"], ws)

    df_excel = df[(~df['Px_Above_MA']) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Below MA"], ws)

    #alpha 30d
    df_excel = df[(df['a_past_8w'] >= 0) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Alpha 8w>=0"], ws)

    df_excel = df[(df['a_past_8w'] < 0) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Alpha 8w<0"], ws)

    # continent
    df_excel = df[(df['Continent'] == 'AMER') & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long AMER"], ws)

    df_excel = df[(df['Continent'] != 'AMER') & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long EMEA"], ws)

    # alpha drop %
    df_excel = df[(df['Alpha'] < 0.1) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Alpha drop -5/-10%"], ws)

    df_excel = df[(df['Alpha'] >= 0.1) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Alpha drop -10% and more"], ws)

    #vol
    df_excel = df[(df['Vol'] > 0.6) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Vol >60%"], ws)

    df_excel = df[(df['Vol'] <= 0.6) & (df['Alto'] == 'B')]
    df_des = get_df_des(df_excel)
    append_excel(df_des, ["Long Vol <=60%"], ws)

wb.save('H:\Python Output\Alto Only\Increase Price Result.xlsx')



