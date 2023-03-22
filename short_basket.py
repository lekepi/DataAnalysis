import pandas as pd


def get_basket_perf(df_price, df_stock, basket_name):

    df = pd.DataFrame()

    if basket_name[-4:] == 'AMER':
        index_name = 'ES1 Index'
    else:
        index_name = 'SXO1 Index'

    for stock in df_price.columns:
        if stock in df_stock['BBG'].tolist():
            df[stock] = df_price[stock]

    # divide all columns by  the first value
    df = df / df.iloc[0, :]
    df2 = df.copy()

    # df['Portfolio'] is the average of all columns
    full_basket_name = basket_name + " Cumul"
    df[full_basket_name] = df.iloc[:, :].mean(axis=1)

    df.insert(0, full_basket_name, df.pop(full_basket_name))

    df[index_name] = df_price[index_name]
    df[index_name] = df[index_name] / df_price[index_name][0]
    # move index_name to the first column
    df.insert(0, index_name, df.pop(index_name))

    # add df_price['Date'] to df as a first column
    df.insert(0, 'Date', df_price['Date'])

    # save to excel in r"H:\Louis Requests\Short Basket\Short Basket Output.xlsx"
    df.to_excel(rf"H:\Louis Requests\Short Basket\Output\{basket_name} Cumul.xlsx", index=False)

    df2 = df2.pct_change() + 1
    full_basket_name2 = basket_name + " Rebalance"
    df2[full_basket_name2] = df2.iloc[:, :].mean(axis=1)
    # put 1 in first column for df['perf']
    df2[full_basket_name2][0] = 1
    # do the cumprod
    df2[full_basket_name2] = df2[full_basket_name2].cumprod()
    # move df['Portfolio'] to the second column
    df2.insert(0, full_basket_name2, df2.pop(full_basket_name2))

    df2[index_name] = df_price[index_name]
    df2[index_name] = df2[index_name] / df_price[index_name][0]
    # move index_name to the first column
    df2.insert(0, index_name, df2.pop(index_name))
    df2.insert(0, 'Date', df_price['Date'])

    df2.to_excel(rf"H:\Louis Requests\Short Basket\Output\{basket_name} Rebalance.xlsx", index=False)


if __name__ == '__main__':

    path = r"H:\Louis Requests\Short Basket\Shorts Backtest Script.xlsm"
    df_price = pd.read_excel(path, sheet_name='BBG')
    # fill na with previous value
    df_price = df_price.fillna(method='ffill')

    df_stock = pd.read_excel(path, sheet_name='Stock List')

    # Group 1
    df_stock_1 = df_stock[df_stock['Group 1'] == 'YES']
    # Region='AMER':
    df_stock_1_AMER = df_stock_1[df_stock_1['Region'] == 'AMER']
    # Region='EMEA':
    df_stock_1_EMEA = df_stock_1[df_stock_1['Region'] != 'AMER']

    # Group 2
    df_stock_2 = df_stock[df_stock['Group 2'] == 'YES']
    # Region='AMER':
    df_stock_2_AMER = df_stock_2[df_stock_2['Region'] == 'AMER']
    # Region='EMEA':
    df_stock_2_EMEA = df_stock_2[df_stock_2['Region'] != 'AMER']

    get_basket_perf(df_price, df_stock_1_AMER, 'Group 1 AMER')
    get_basket_perf(df_price, df_stock_1_EMEA, 'Group 1 EMEA')
    get_basket_perf(df_price, df_stock_2_AMER, 'Group 2 AMER')
    get_basket_perf(df_price, df_stock_2_EMEA, 'Group 2 EMEA')
