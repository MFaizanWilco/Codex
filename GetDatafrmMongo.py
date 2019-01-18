from Depend import *
# from ControllerApi import *
from Config import *
import mongodb
import pandas as pd
from dateutil import parser
from datetime import timedelta


class GetData_ib:
    def __init__(self):
        self.name = "Stock_Data"
        self.Company = Configuration().GetData()['CompanyList']


    def display_IntraDay_Data(self, company_name, barSize, counter):
        try:
            collection = ''
            if barSize == '5 secs':
                collection = 'IntraDay_5s'
            elif barSize == '15 secs':
                collection='IntraDay_15s'
            elif barSize == '30 secs':
                collection ='IntraDay_30s'
            elif barSize == '1 min':
                collection = 'IntraDay_1m'
            elif barSize == '2 min':
                collection = 'IntraDay_2m'
            elif barSize == '3 min':
                collection = 'IntraDay_3m'
            elif barSize == '5 min':
                collection = 'IntraDay_5m'
            elif barSize == '15 min':
                collection = 'IntraDay_15m'
            elif barSize == '30 min':
                collection = 'IntraDay_30m'
            elif barSize == '1 hour':
                collection = 'IntraDay_1h'
            elif barSize == '1 day':
                collection = 'IntraDay'



            value = mongodb.ReadValue(collection, company_name)['Data']
            df = pd.DataFrame(eval(value))
            # data = df[(df['date'] > start_date) & (df['date'] < end_date)]

            date_filter = (df['date'].max()).split(' ')[0]
            dt = parser.parse(date_filter).date()

            df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close"},
                      inplace=True)

            df['date'] = pd.to_datetime(df['date'])

            df['_date'] = [d.date() for d in df['date']]
            df['_time'] = [t.time() for t in df['date']]

            df['date'] = df['date'].astype(str)
            df['_date'] = df['_date'].astype(str)
            df['_time'] = df['_time'].astype(str)

            filter_date = dt - timedelta(days=int(counter))

            p_df = self.temp(company_name, filter_date)
            # p_df = pd.DataFrame(eval(value))
            # p_df.rename(columns={"date": "dateP"}, inplace=True)
            final_df = pd.merge(df, p_df, how='left', left_on='date', right_on='date')
            # print(final_df)
            final_df = final_df.fillna('')
            # temp_list = [data.to_dict(orient='list'), p_df.to_dict(orient='list')]
            #
            # final_df = pd.concat([data,p_df])
            # return str(final_df.to_dict(orient='list'))
            final_df = final_df[final_df['date'].str.contains(str(filter_date))]
            return final_df.to_dict(orient='list')
            # return temp_list
        except Exception as e:
            return 'False'

    def temp(self, company_name, filter_date):
        try:
            collectionname = 'PredictionStore'
            value = mongodb.ReadValue(collectionname, company_name + ' ' + str(filter_date))['Data']
            df = pd.DataFrame(eval(value))
        except:
            df = pd.DataFrame([], columns=['date', 'high', 'low', 'open', 'close'])
        return df

    def display_Daily_Data(self, company_name):
        try:
            collection = 'Daily'
            value = mongodb.ReadValue(collection, company_name)['Data']
            df = pd.DataFrame(eval(value))
            df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close"},
                      inplace=True)
            df = df.tail(30)
            return df.to_dict(orient='list')
        except Exception as e:
            return 'False'

    def display_Weekly_Data(self, company_name):
        try:
            collection = 'Weekly'
            value = mongodb.ReadValue(collection, company_name)['Data']
            df = pd.DataFrame(eval(value))
            df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close"},
                      inplace=True)
            df = df.tail(48)
            return df.to_dict(orient='list')
        except Exception as e:
            print(str(e))
            return 'False'

    def display_Monthly_Data(self, company_name):
        try:
            collection = 'Monthly'
            value = mongodb.ReadValue(collection, company_name)['Data']
            df = pd.DataFrame(eval(value))
            df.rename(columns={"0":"Date","1": "open", "2": "high", "3": "low", "4": "close"},
                      inplace=True)
            df = df.tail(12)
            return df.to_dict(orient='list')
        except Exception as e:
            return 'False'

# elif action == 'getAdjClose':
#     try:
#         company_symbol = data['ticket_name']
#         feed = quandlfeed.Feed()
#         feed.addBarsFromCSV(company_symbol, company_symbol + ".csv")
#         myStrategy = MyStrategy(feed, company_symbol)
#         myStrategy.run()
#         return str(myStrategy.get_AdjClose())
#     except Exception as e:
#         return 'False'
#
# elif action == 'getFrequency':
#     try:
#         company_symbol = data['ticket_name']
#         feed = quandlfeed.Feed()
#         feed.addBarsFromCSV(company_symbol, company_symbol + ".csv")
#         myStrategy = MyStrategy(feed, company_symbol)
#         myStrategy.run()
#         return str(myStrategy.get_Frequency())
#     except Exception as e:
#         return 'False'
#
# elif action == 'getTypicalPrice':
#     try:
#         company_symbol = data['ticket_name']
#         feed = quandlfeed.Feed()
#         feed.addBarsFromCSV(company_symbol, company_symbol + ".csv")
#         myStrategy = MyStrategy(feed, company_symbol)
#         myStrategy.run()
#         return str(myStrategy.get_TypicalPrice())
#     except Exception as e:
#         return 'False'
#
# elif action == 'getPrice':
#     try:
#         company_symbol = data['ticket_name']
#         feed = quandlfeed.Feed()
#         feed.addBarsFromCSV(company_symbol, company_symbol + ".csv")
#         myStrategy = MyStrategy(feed, company_symbol)
#         myStrategy.run()
#         return str(myStrategy.get_Price())
#     except Exception as e:
#         return 'False'
#