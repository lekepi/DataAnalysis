from datetime import date
import pandas as pd
from models import engine, session, NavAccountStatement
from utils import simple_email


def get_mtd_class_b(my_date, investor_nav_id_list, extra_lines):

    result_list = []

    # get all records for the investor_nav_id_list
    my_sql = f"""SELECT entry_date,investor_nav_id,investor_number,class_name,serie,fund_type,beginning_balance,additions,
        redemptions,total_income,management_fee,incentive_fee,ending_balance,return_rate,redemption_type
        FROM investor_capital WHERE investor_nav_id in ({investor_nav_id_list}) and entry_date<'{my_date}'
        order by investor_nav_id,investor_number,entry_date;"""
    df = pd.read_sql(my_sql, con=engine, parse_dates=['entry_date'])

    # get all distinct investor_number in a list
    investor_number_list = df['investor_number'].unique().tolist()

    for investor_number in investor_number_list:
        df_temp = df[df['investor_number'] == investor_number]
        start_date = df_temp.iloc[0]['entry_date'].date()
        last_record = df_temp.iloc[-1]
        last_record_date = last_record['entry_date']
        # if last_record_date is less than 30 days before my_date, skip
        if (my_date - last_record_date.date()).days < 33:
            # get first record from df_temp
            first_record = df_temp.iloc[0]
            class_name = first_record['class_name']
            fund_type = first_record['fund_type']
            if fund_type == 'GLD':
                fund_type = 'USD GLD'

            serie = first_record['serie']

            if class_name in ['L', 'B']:
                data_name = f'RETURN {fund_type} CLASS L'
            elif class_name in ['F', 'A']:
                data_name = f'RETURN {fund_type} CLASS F'
            nav_statement = session.query(NavAccountStatement).filter(NavAccountStatement.data_name == data_name). \
                filter(NavAccountStatement.entry_date == my_date).filter(NavAccountStatement.status == 'Daily').first()
            mtd_perf = nav_statement.data_mtd

            if class_name in ['L', 'F']:
                perf_mtd_result = mtd_perf
            elif class_name in ['A', 'B']:
                # get distinct year before this year
                year_list = df_temp['entry_date'].dt.year.unique().tolist()
                # remove my_date.year from the list
                if my_date.year in year_list:
                    year_list.remove(my_date.year)

                last_ending_balance = df_temp.iloc[-1]['ending_balance']

                # if sum (incentive_fee) for the year is >0, remove that year from the list - fees counted
                for my_year in year_list:
                    # if sum of incentive_fee for the year is >0, remove that year from df_temp
                    if df_temp[df_temp['entry_date'].dt.year == my_year]['incentive_fee'].sum() > 0:
                        df_temp = df_temp[df_temp['entry_date'].dt.year != my_year]

                if df_temp.empty and my_date.month == 1: # When we are in January (no line yet) after a reset of inc. fees
                    amount_limit = last_ending_balance * 1.05  # 5% hurdle
                    incentive_fee_this_month = max(0, 0.15 * ((last_ending_balance) * (1 + mtd_perf / 100) - amount_limit))
                    incentive_fee_this_month_perc = incentive_fee_this_month / last_ending_balance
                else:
                    year_list = df_temp['entry_date'].dt.year.unique().tolist()
                    # calculate the
                    begin_capital = None
                    total_hurdle = 0
                    for my_year in year_list:
                        # get begin capital for the year
                        begin_capital_temp = df_temp[df_temp['entry_date'].dt.year == my_year]['beginning_balance'].iloc[0]
                        additions_temp = df_temp[df_temp['entry_date'].dt.year == my_year]['additions'].iloc[0]
                        begin_capital_temp = begin_capital_temp + additions_temp
                        if begin_capital is None:
                            begin_capital = begin_capital_temp
                        hurdle = begin_capital_temp * 0.05
                        total_hurdle = total_hurdle + hurdle
                    amount_limit = total_hurdle + begin_capital

                    begin_last_month = df_temp['ending_balance'].iloc[-1]

                    # get sum df_temp['incentive_fee'] for my_year

                    previous_incentive_fee_ytd = df_temp[df_temp['entry_date'].dt.year == my_year]['incentive_fee'].sum()
                    incentive_fee_ytd = max(0, 0.15 * ((begin_last_month + previous_incentive_fee_ytd) * (1+mtd_perf/100) - amount_limit))

                    incentive_fee_this_month = incentive_fee_ytd - previous_incentive_fee_ytd
                    incentive_fee_this_month_perc = incentive_fee_this_month / begin_last_month

                perf_mtd_result = 100 * (mtd_perf/100 - incentive_fee_this_month_perc)
            result_list.append([fund_type, class_name, serie, start_date, perf_mtd_result])

    for extra_line in extra_lines:
        fund_type = extra_line[0]
        class_name = extra_line[1]
        if class_name in ['L', 'B']:
            data_name = f'RETURN {fund_type} CLASS L'
        elif class_name in ['F', 'A']:
            data_name = f'RETURN {fund_type} CLASS F'
        nav_statement = session.query(NavAccountStatement).filter(NavAccountStatement.data_name == data_name). \
            filter(NavAccountStatement.entry_date == my_date).filter(NavAccountStatement.status == 'Daily').first()
        mtd_perf = nav_statement.data_mtd
        extra_line.append(mtd_perf)
        result_list.append(extra_line)

    df_result = pd.DataFrame(result_list, columns=['Fund Type', 'Class Name', 'Serie', 'Entry Date', 'perf_mtd'])
    # sort by fund type, class name, serie
    df_result = df_result.sort_values(by=['Fund Type', 'Class Name', 'Serie'])
    # round perf_mtd at 2 decimals
    df_result['perf_mtd'] = df_result['perf_mtd'].round(2).astype(str) + '%'

    # send email
    html = f"<h3>Performance as of {my_date.strftime('%B %d, %Y')}</h3>"
    html = html + df_result.to_html(index=False, justify='center')
    simple_email("ALTO Month to date estimate", '', 'olivier@ananda-am.com', html=html)


if __name__ == '__main__':

    my_date = date(2023, 2, 28)
    investor_nav_id_list = '14'
    # extra_lines = [['EURO', 'A', 'New', date(2022, 9, 1)]]
    extra_lines = []
    get_mtd_class_b(my_date, investor_nav_id_list, extra_lines)