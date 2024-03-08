from datetime import date
import pandas as pd
from models import engine, session, Factor, FactorDriver, FactorPerf
from utils import task_checker_db


def get_factor_driver(my_date):
    session.query(FactorPerf).delete()
    # delete FactorDriver where entry_date = my_date
    session.query(FactorDriver).filter(FactorDriver.entry_date == my_date).delete()
    session.commit()

    region = ['AMER', 'EMEA']
    side = ['Long', 'Short']

    # filename is the name of the file in 'H:\Factors\BBU\Download\BBU 2023-03-09.xlsx'
    filename = rf'H:\Factors\BBU\Download\BBU {my_date}.xlsx'

    factor_list = session.query(Factor).all()

    error_list = []
    factor_driver_list = []
    factor_perf_list = []

    for r in region:
        for s in side:
            df = pd.read_excel(filename, sheet_name=f'ALTO_{r} {s}', header=3)
            # in df['Name'] column, replace '%' by 'Perc'
            df['Name'] = df['Name'].str.replace('%', 'Perc')

            for factor in factor_list:
                factor_name = factor.name

                # filter the row in the dataframe where Name = factor_name
                df_factor = df[df['Name'] == factor_name]
                # if df is null add to error_list
                if df_factor.empty:
                    error_list.append(f"{r} {s}: '{factor_name}' missing.")
                else:
                    # get the quintile values
                    quintile1 = df_factor['Low'].values[0]
                    quintile2 = df_factor['Mid-L'].values[0]
                    quintile3 = df_factor['Mid'].values[0]
                    quintile4 = df_factor['Mid-H'].values[0]
                    quintile5 = df_factor['High'].values[0]
                    non_applicable = df_factor['N.A.'].values[0]
                    strength = df_factor['Strength'].values[0]

                    new_factor_driver = FactorDriver(entry_date=my_date,
                                                     region=r,
                                                     side=s,
                                                     factor_id=factor.id,
                                                     quintile1=quintile1,
                                                     quintile2=quintile2,
                                                     quintile3=quintile3,
                                                     quintile4=quintile4,
                                                     quintile5=quintile5,
                                                     non_applicable=non_applicable,
                                                     strength=strength)
                    factor_driver_list.append(new_factor_driver)

    for r in region:
        df = pd.read_excel(filename, sheet_name=f'Perf {r}', parse_dates=['Date'])
        # convert the 'Date' column to date
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        # sort by Date
        df = df.sort_values(by='Date')
        # reset index
        df = df.reset_index(drop=True)

        # remove ' Long-Short (High-Low) Total Return' from the dataframe column name
        df.columns = df.columns.str.replace(' Long-Short (High-Low) Total Return', '')
        df.columns = df.columns.str.replace('FTW SPX Index ', '')
        df.columns = df.columns.str.replace('FTW SXXP Index ', '')

        for factor in factor_list:
            factor_name = factor.name
            # loop through the dataframe and add a record in FactorPerf table
            # check if column factor_name exists in dataframe
            if factor_name not in df.columns:
                error_list.append(f'{r}: {factor_name} Perf missing.')
            else:
                # from df, get the value for factor_name column for Date=last_entry_date and compare

                for index, row in df.iterrows():
                    if index > 0:  # skip the first row
                        # get the date and perf value
                        perf_date = row['Date']
                        perf_value = row[factor_name]
                        # Add record in FactorPerf table
                        new_factor_perf = FactorPerf(entry_date=perf_date,
                                                        region=r,
                                                        factor_id=factor.id,
                                                        perf=perf_value)
                        factor_perf_list.append(new_factor_perf)


    session.add_all(factor_driver_list)
    session.add_all(factor_perf_list)
    session.commit()
    print('Factor Driver updated successfully.')
    task_checker_db(status='Success', task_details=f'Factor Driver - {my_date}',
                    comment=f"Factor Driver updated successfully.",
                    task_name='Factor Driver', task_type='DataAnalysis')


if __name__ == '__main__':

    # my_date = date(2023, 3, 27)
    my_date = date.today()
    get_factor_driver(my_date)
