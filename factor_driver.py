from datetime import date
import pandas as pd
from models import engine, session, Factor, FactorDriver, FactorPerf
from utils import task_checker_db

def get_factor_driver(my_date):

    # delete FactorDriver where entry_date = my_date
    session.query(FactorDriver).filter(FactorDriver.entry_date == my_date).delete()
    session.query(FactorPerf).filter(FactorPerf.entry_date == my_date).delete()
    session.commit()

    region = ['AMER', 'EMEA']
    side = ['Long', 'Short']

    # filename is the name of the file in 'H:\Factors\BBU\Download\BBU 2023-03-09.xlsx'
    filename = rf'H:\Factors\BBU\Download\BBU {my_date}.xlsx'

    factor_list = session.query(Factor).all()

    error_list = []
    factor_driver_list = []

    for r in region:
        for s in side:
            df = pd.read_excel(filename, sheet_name=f'ALTO_{r} {s}', header=3)

            for factor in factor_list:
                factor_name = factor.name

                # filter the row in the dataframe where Name = factor_name
                df_factor = df[df['Name'] == factor_name]
                # if df is null add to error_list
                if df_factor.empty:
                    error_list.append(f'{r} {s}: {factor_name} missing.')
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

    # update factor_perf table
    factor_perf_list = []
    for r in region:
        df = pd.read_excel(filename, sheet_name=f'Perf {r}', header=3)
        for factor in factor_list:
            factor_name = factor.name
            # rename the second column of the dataframe to 'Name'
            df.rename(columns={df.columns[1]: 'Name'}, inplace=True)

            # filter the row in the dataframe where Name = factor_name
            df_factor = df[df['Name'] == factor_name]
            # if df is null add to error_list
            if df_factor.empty:
                error_list.append(f'{r}: {factor_name} Perf missing.')
            else:
                # get the quintile values
                perf_1w = df_factor['Prior Wk'].values[0]
                perf_1m = df_factor['1Mo'].values[0]
                perf_3m = df_factor['3Mo'].values[0]
                perf_6m = df_factor['6Mo'].values[0]

                # Add record in FactorPerf table
                new_factor_perf = FactorPerf(entry_date=my_date,
                                             region=r,
                                             factor_id=factor.id,
                                             perf_1w=perf_1w,
                                             perf_1m=perf_1m,
                                             perf_3m=perf_3m,
                                             perf_6m=perf_6m)
                factor_perf_list.append(new_factor_perf)

    if error_list:
        print('Error List:')
        for error in error_list:
            print(error)
    else:
        session.add_all(factor_driver_list)
        session.add_all(factor_perf_list)
        session.commit()
        print('Factor Driver updated successfully.')
        task_checker_db(status='Success', task_details=f'Factor Driver - {my_date}',
                        comment=f"Factor Driver updated successfully.",
                        task_name='Factor Driver', task_type='DataAnalysis')


if __name__ == '__main__':
    my_date = date(2023, 3, 9)
    # my_date = date.today()

    get_factor_driver(my_date)
