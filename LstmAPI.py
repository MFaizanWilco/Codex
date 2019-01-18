import numpy
import pandas as pd
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
from Config import Configuration
import mongodb
import stocksLSTM
from datetime import datetime
from dateutil import parser
from datetime import timedelta
import calendar

class lstm():

    def __init__(self):
        self.name = 'LstmAPI'

    def getNextDate(self, currrentDate):
        curDate = parser.parse(currrentDate).date()
        nextDay = curDate + timedelta(days=int(1))
        weekend = ['Saturday', 'Sunday']

        while calendar.day_name[nextDay.weekday()] in weekend:
            nextDay = nextDay + timedelta(days=int(1))
        return nextDay

    def selected_predict(self, company_name, start_date, end_date, nopredictions):
            try:
                collectionname = 'IntraDay'
                t1 = start_date.split(' ')[1]
                t2 = end_date.split(' ')[1]
                FMT = '%H:%M:%S'
                tdelta = datetime.strptime(t2, FMT) - datetime.strptime(t1, FMT)
                total_minutes = int(tdelta.total_seconds() / 60)
                value = mongodb.ReadValue(collectionname, company_name)['Data']
                df = pd.DataFrame(eval(value))
                df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close",
                                   "5. volume": "volume"}, inplace=True)

                if 'volume' in df.columns:
                    del df['volume']

                test_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

                max_date = (test_df['date'].max()).split(' ')[0]
                max_time = (test_df['date'].max()).split(' ')[1]

                testEnd = test_df
                trainStart = df.tail(312)

                # dates = df['date'].copy()
                del testEnd['date']
                del trainStart['date']

                trainStart, scaler = stocksLSTM.ScaleDataSet(trainStart)
                testEnd, _ = stocksLSTM.ScaleDataSet(testEnd, scaler)

                trainStart = stocksLSTM.prepareDataSet(trainStart)

                model, details = stocksLSTM.trainModel(trainStart)

                presults = stocksLSTM.predict(model, testEnd)
                presults = stocksLSTM.deScaleData(presults, scaler)

                temp_time = datetime.strptime(max_time, "%H:%M:%S")
                time = temp_time.time()
                gen_date = None
                rng = None
                global frequency
                global points

                if nopredictions == 5:
                    frequency = '5min'
                    points = total_minutes / 5
                elif nopredictions == 15:
                    frequency = '15min'
                    points = total_minutes / 15
                elif nopredictions == 30:
                    frequency = '30min'
                    points = total_minutes / 30
                elif nopredictions == 60:
                    frequency = '60min'
                    points = total_minutes / 60

                if time.hour == 16:
                    gen_date = self.getNextDate(max_date)
                    rng = pd.date_range(str(gen_date) + ' ' + '09:35:00', periods=points, freq=frequency)
                else:
                    rng = pd.date_range(str(max_date) + ' ' + str(max_time), periods=points, freq=frequency)

                ts = pd.Series(rng)
                presults['date'] = ts
                presults['date'] = presults['date'].astype(str)
                presults.rename(columns={"openP": "open", "highP": "high", "lowP": "low", "closeP": "close"},
                                inplace=True)
                return presults.to_dict(orient='list')
            except Exception as e:
                print(str(e))
                return 'False'

        # --------------------------------------------------------------------------------------------------------------

        # dataframe = df.reset_index(drop=True)
        # dates = dataframe['date'].copy()
        # del dataframe['date']
        # seedValue = dataframe.tail(1)
        # dataframe, scaler = stocksLSTM.ScaleDataSet(dataframe)
        # dataframe = stocksLSTM.prepareDataSet(dataframe)
        # model, details = stocksLSTM.trainModel(dataframe)
        # seedValue, _ = stocksLSTM.ScaleDataSet(seedValue, scaler)
        #
        # p_df = stocksLSTM.predictfulDay(model, seedValue, nopredictions)
        # p_df = stocksLSTM.deScaleData(p_df, scaler)
        # # print(p_df)
        # temp_time = datetime.strptime(max_time, "%H:%M:%S")
        # time = temp_time.time()
        # gen_date = None
        # rng = None
        # if time.hour == 16:
        #     gen_date = self.getNextDate(max_date)
        #     rng = pd.date_range(str(gen_date) + ' ' + '09:35:00', periods=nopredictions, freq='5min')
        #     ts = pd.Series(rng)
        # else:
        #     rng = pd.date_range(str(max_date) + ' ' + str(max_time), periods=nopredictions, freq='5min')
        #     # dt_list = list(pd.Series(rng))
        #     # new_time_list = []
        #     # for x in dt_list:
        #     #     if x.hour >= 16:
        #     #         new_time_list.append(x.replace(hour=9, minute=x.minute))
        #     #     else:
        #     #         new_time_list.append(x)
        #
        # ts = pd.Series(rng)
        # p_df['date'] = ts
        # p_df['date'] = p_df['date'].astype(str)
        # p_df.rename(columns={"openP": "open", "highP": "high", "lowP": "low", "closeP": "close"}, inplace=True)
        # return p_df.to_dict(orient='list')
0