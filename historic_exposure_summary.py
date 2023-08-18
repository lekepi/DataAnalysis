from datetime import date, timedelta
import pandas as pd
from models import engine, session, Aum

def get_eom_weekday_list():
    # Get historic exposure summary from the database
    date_list = []

    start_date = date(2019, 4, 1)
    today = date.today()

    # find last date of last month from today
    final_day = today.replace(day=1) - timedelta(days=1)


    # Loop through each month
    while start_date < final_day:
        end_of_month = start_date.replace(day=28) + timedelta(days=4)
        last_day = end_of_month - timedelta(days=end_of_month.day)
        # if last_day is a weekend go back to Friday
        if last_day.weekday() == 5:
            last_day = last_day - timedelta(days=1)
        elif last_day.weekday() == 6:
            last_day = last_day - timedelta(days=2)

        date_list.append(last_day)
        # Move to the next month
        start_date = start_date.replace(day=1) + timedelta(days=32)
        start_date = start_date.replace(day=1)
    return date_list


def get_historic_exposure_summary():
    date_list = get_eom_weekday_list()

    aum_list = session.query(Aum).filter(Aum.type == 'leveraged').all()
    exposure_list = []
    country_list = []
    sector_list = []

    for date in date_list:
        print(date)
        my_sql = f"""SELECT entry_date,product_id,beta,T2.ticker,T2.prod_type,T4.name as country,continent,T5.name as sector,mkt_value_usd FROM position T1
                    JOIN product T2 on T1.product_id=T2.id JOIN exchange T3 on T2.exchange_id=T3.id
                    JOIN country T4 on T3.country_id=T4.id LEFT JOIN industry_sector T5 on T2.industry_sector_id=T5.id
                    WHERE entry_date='{date}' and parent_fund_id=1 and prod_type in ('Cash','Future') 
                    and T2.ticker not in ('AGI US', 'FNV US','FNV CN','NEM US','GOLD US','AEM US','GDX US','GC1 CMX','GLD US');"""
        df = pd.read_sql(my_sql, con=engine)
        # replace beta with 1 if it is null
        df['beta'] = df['beta'].fillna(1)

        # replace entry_date with the date of the first day of the month
        aum_date = df['entry_date'][0]
        # get the Aum object for the date
        # get the aum element where entry_date month and year are the same as aum_date
        aum = [aum.amount for aum in aum_list if aum.entry_date.month == aum_date.month and aum.entry_date.year == aum_date.year][0]*1000000

        # get the sum of mkt_value_usd when it is >0:
        sum_long = df[df['mkt_value_usd'] > 0]['mkt_value_usd'].sum()
        # get the sum of mkt_value_usd when it is <0:
        sum_short = df[df['mkt_value_usd'] < 0]['mkt_value_usd'].sum()

        df['beta_usd'] = df['beta'] * df['mkt_value_usd']
        net_beta = df['beta_usd'].sum()


        exposure_list.append({'date': aum_date, 'long': sum_long / aum, 'short': sum_short / aum,
                             'net': (sum_long + sum_short) / aum, 'net_beta': net_beta / aum ,
                             'gross': (sum_long - sum_short) / aum})

        # convert exposure_list into a dataframe
        df_exposure = pd.DataFrame(exposure_list)
        # save into excel file
        df_exposure.to_excel('historic_exposure_summary.xlsx', index=False)

        #keep only mkts with mkt_value_usd >0
        df = df[df['mkt_value_usd'] > 0]
        all_total = df['mkt_value_usd'].sum()
        us_total = df[df['country'] == 'United States']['mkt_value_usd'].sum()
        uk_total = df[df['country'] == 'United Kingdom']['mkt_value_usd'].sum()
        france_total = df[df['country'] == 'France']['mkt_value_usd'].sum()
        switzerland_total = df[df['country'] == 'Switzerland']['mkt_value_usd'].sum()
        norway_total = df[df['country'] == 'Norway']['mkt_value_usd'].sum()
        sweden_total = df[df['country'] == 'Sweden']['mkt_value_usd'].sum()
        netherlands_total = df[df['country'] == 'Netherlands']['mkt_value_usd'].sum()
        canada_total = df[df['country'] == 'Canada']['mkt_value_usd'].sum()
        spain_total = df[df['country'] == 'Spain']['mkt_value_usd'].sum()
        germany_total = df[df['country'] == 'Germany']['mkt_value_usd'].sum()
        ireland_total = df[df['country'] == 'Ireland']['mkt_value_usd'].sum()
        denmark_total = df[df['country'] == 'Denmark']['mkt_value_usd'].sum()
        italy_total = df[df['country'] == 'Italy']['mkt_value_usd'].sum()
        other = all_total - us_total - uk_total - france_total - switzerland_total - norway_total - sweden_total - \
                netherlands_total - canada_total - spain_total - germany_total - ireland_total - denmark_total - italy_total

        amer_total = us_total + canada_total
        emea_total = all_total - amer_total

        country_list.append({'date': aum_date, 'amer': amer_total / all_total, 'emea': emea_total / all_total,
                             'us': us_total / all_total, 'uk': uk_total / all_total,
                             'france': france_total / all_total, 'switzerland': switzerland_total / all_total,
                             'norway': norway_total / all_total, 'sweden': sweden_total / all_total,
                             'netherlands': netherlands_total / all_total, 'canada': canada_total / all_total,
                             'spain': spain_total / all_total, 'germany': germany_total / all_total,
                             'ireland': ireland_total / all_total, 'denmark': denmark_total / all_total,
                             'italy': italy_total / all_total, 'other': other / all_total})

        # convert country_list into a dataframe
        df_country = pd.DataFrame(country_list)
        # save into excel file
        df_country.to_excel('historic_country_summary.xlsx', index=False)

        consumer_nc = df[df['sector'] == 'Consumer, Non-cyclical']['mkt_value_usd'].sum()
        consumer_c = df[df['sector'] == 'Consumer, Cyclical']['mkt_value_usd'].sum()
        industrial = df[df['sector'] == 'Industrial']['mkt_value_usd'].sum()
        Financial = df[df['sector'] == 'Financial']['mkt_value_usd'].sum()
        energy = df[df['sector'] == 'Energy']['mkt_value_usd'].sum()
        basic_materials = df[df['sector'] == 'Basic Materials']['mkt_value_usd'].sum()
        communications = df[df['sector'] == 'Communications']['mkt_value_usd'].sum()
        technology = df[df['sector'] == 'Technology']['mkt_value_usd'].sum()
        utilities = df[df['sector'] == 'Utilities']['mkt_value_usd'].sum()

        sector_list.append({'date': aum_date, 'consumer_nc': consumer_nc / all_total, 'consumer_c': consumer_c / all_total,
                           'industrial': industrial / all_total, 'financial': Financial / all_total,
                           'energy': energy / all_total, 'basic_materials': basic_materials / all_total,
                           'communications': communications / all_total, 'technology': technology / all_total,
                           'utilities': utilities / all_total})

        # convert sector_list into a dataframe
        df_sector = pd.DataFrame(sector_list)
        # save into excel file
        df_sector.to_excel('historic_sector_summary.xlsx', index=False)


if __name__ == "__main__":
    get_historic_exposure_summary()
