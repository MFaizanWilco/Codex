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
       self.name = 'LSTM'

    def predict(self, company):
        try:
            collectionname = 'FuturePrediction'
            value = mongodb.ReadValue(collectionname, company)['Data']
            df = pd.DataFrame(eval(value))
            df.rename(columns={"openP": "open", "highP": "high", "lowP": "low", "closeP": "close","volumeP":"volume"}, inplace=True)
            return df.head().to_dict(orient='list')
        except Exception as e:
            print(str(e))
            return str(e)

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
            presults.rename(columns={"openP": "open", "highP": "high", "lowP": "low", "closeP": "close"}, inplace=True)
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


#     def predict(self, collection, company_name):
#       try:
#           def create_dataset(dataset, look_back=1):
#               dataX, dataY = [], []
#               for i in range(len(dataset) - look_back - 1):
#                   a = dataset[i:(i + look_back), 0]
#                   dataX.append(a)
#                   dataY.append(dataset[i + look_back, 0])
#               return numpy.array(dataX), numpy.array(dataY)
#
#           # fix random seed for reproducibility
#           numpy.random.seed(7)
#
#           # load the dataset
#           value = mongodb.ReadValue(collection, company_name)['Data']
#           df = pd.DataFrame(eval(value))
#           # print(df)
#
#           df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close"}, inplace=True)
#
#           if '5. volume' in df.columns:
#               del df['5. volume']
#
#           df = df.tail(250)
#           dataframe = df.reset_index(drop=True)
#
#           dates = dataframe['date'].copy()
#           del dataframe['date']
#
#           # dataset = dataframe.values
#           dataset = dataframe.astype('float32')
#           # normalize the dataset
#           scaler = MinMaxScaler(feature_range=(0, 1))
#           dataset_tran = scaler.fit_transform(dataset.values)
#           dataset = pd.DataFrame(dataset_tran, index=dataset.index, columns=dataset.columns)
#           # print(dataset)
#           # split into train and test sets
#           train_size = int(dataset.shape[0] * 0.72)
#           test_size = dataset.shape[0] - train_size
#           train, test = dataset.loc[0:train_size], dataset.loc[train_size:dataset.shape[0]]
#           # reshape into X=t and Y=t+1
#           look_back = 1
#           trainOpenX, trainOpenY = create_dataset(numpy.reshape(train['open'].values, (train['open'].shape[0], 1)),
#                                                   look_back)
#           trainHighX, trainHighY = create_dataset(numpy.reshape(train['high'].values, (train['high'].shape[0], 1)),
#                                                   look_back)
#           trainLowX, trainLowY = create_dataset(numpy.reshape(train['low'].values, (train['low'].shape[0], 1)),
#                                                 look_back)
#           trainCloseX, trainCloseY = create_dataset(numpy.reshape(train['close'].values, (train['close'].shape[0], 1)),
#                                                     look_back)
#
#           testOpenX, testOpenY = create_dataset(numpy.reshape(test['open'].values, (test['open'].shape[0], 1)),
#                                                 look_back)
#           testHighX, testHighY = create_dataset(numpy.reshape(test['high'].values, (test['high'].shape[0], 1)),
#                                                 look_back)
#           testLowX, testLowY = create_dataset(numpy.reshape(test['low'].values, (test['low'].shape[0], 1)), look_back)
#           testCloseX, testCloseY = create_dataset(numpy.reshape(test['close'].values, (test['close'].shape[0], 1)),
#                                                   look_back)
#           # reshape input to be [samples, time steps, features]
#
#           trainOpenX = numpy.reshape(trainOpenX, (trainOpenX.shape[0], 1, trainOpenX.shape[1]))
#           trainHighX = numpy.reshape(trainHighX, (trainHighX.shape[0], 1, trainHighX.shape[1]))
#           trainLowX = numpy.reshape(trainLowX, (trainLowX.shape[0], 1, trainLowX.shape[1]))
#           trainCloseX = numpy.reshape(trainCloseX, (trainCloseX.shape[0], 1, trainCloseX.shape[1]))
#
#           testOpenX = numpy.reshape(testOpenX, (testOpenX.shape[0], 1, testOpenX.shape[1]))
#           testHighX = numpy.reshape(testHighX, (testHighX.shape[0], 1, testHighX.shape[1]))
#           testLowX = numpy.reshape(testLowX, (testLowX.shape[0], 1, testLowX.shape[1]))
#           testCloseX = numpy.reshape(testCloseX, (testCloseX.shape[0], 1, testCloseX.shape[1]))
#
#           # create and fit the LSTM network
#           model = Sequential()
#           model.add(LSTM(16, input_shape=(1, look_back)))
#           model.add(Dense(1))
#           model.compile(loss='mean_squared_error', optimizer='adam')
#           model.fit(trainOpenX, trainOpenY, epochs=10, batch_size=1, verbose=0)
#           trainOpenPredict = model.predict(trainOpenX)
#           testOpenPredict = model.predict(testOpenX)
#
#           model.fit(trainHighX, trainHighY, epochs=10, batch_size=1, verbose=0)
#           trainHighPredict = model.predict(trainHighX)
#           testHighPredict = model.predict(testHighX)
#
#           model.fit(trainLowX, trainLowY, epochs=10, batch_size=1, verbose=0)
#           trainLowPredict = model.predict(trainLowX)
#           testLowPredict = model.predict(testLowX)
#
#           model.fit(trainCloseX, trainCloseY, epochs=10, batch_size=1, verbose=0)
#           trainClosePredict = model.predict(trainCloseX)
#           testClosePredict = model.predict(testCloseX)
#
#           Train_m_Predict = numpy.concatenate([trainOpenPredict, trainHighPredict, trainLowPredict, trainClosePredict],
#                                               axis=1)
#           Test_m_Predict = numpy.concatenate([testOpenPredict, testHighPredict, testLowPredict, testClosePredict],
#                                              axis=1)
#
#           trainPredict = scaler.inverse_transform(Train_m_Predict)
#           testPredict = scaler.inverse_transform(Test_m_Predict)
#
#
#           df_trainP = pd.DataFrame(trainPredict, columns=dataset.columns)
#           df_testP = pd.DataFrame(testPredict, columns=dataset.columns)
#
#           frames = [df_trainP, df_testP]
#           finalSet = pd.concat(frames, ignore_index=True)
#           finalSet['date'] = dates
#
#           # finalSet = finalSet[(finalSet['date'] > start_date) & (finalSet['date'] < end_date)]
#
#           date_filter = (finalSet['date'].max()).split(' ')[0]
#
#           finalSet = finalSet[finalSet['date'].str.contains(date_filter)]
#
#           return finalSet.to_dict(orient='list')
#
#       except Exception as e:
#          print(str(e))
#          return str(e)
#
#
# # '''-----------------------------------Selected Prediction--------------------------------------------'''
#
#     def selected_predict(self, collection, company_name, start_date, end_date):
#         try:
#             def create_dataset(dataset, look_back=1):
#                 dataX, dataY = [], []
#                 for i in range(len(dataset) - look_back - 1):
#                     a = dataset[i:(i + look_back), 0]
#                     dataX.append(a)
#                     dataY.append(dataset[i + look_back, 0])
#                 return numpy.array(dataX), numpy.array(dataY)
#
#             # fix random seed for reproducibility
#             numpy.random.seed(7)
#
#             # load the dataset
#             value = mongodb.ReadValue(collection, company_name)['Data']
#             df = pd.DataFrame(eval(value))
#             # print(df)
#             df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
#             # print(df)
#
#             df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close"},
#                       inplace=True)
#
#             if '5. volume' in df.columns:
#                 del df['5. volume']
#
#             dataframe = df.reset_index(drop=True)
#
#             dates = dataframe['date'].copy()
#             del dataframe['date']
#
#             # dataset = dataframe.values
#             dataset = dataframe.astype('float32')
#             # normalize the dataset
#             scaler = MinMaxScaler(feature_range=(0, 1))
#             dataset_tran = scaler.fit_transform(dataset.values)
#             dataset = pd.DataFrame(dataset_tran, index=dataset.index, columns=dataset.columns)
#             # print(dataset)
#             # split into train and test sets
#             train_size = int(dataset.shape[0] * 0.72)
#             test_size = dataset.shape[0] - train_size
#             train, test = dataset.loc[0:train_size], dataset.loc[train_size:dataset.shape[0]]
#             # reshape into X=t and Y=t+1
#             look_back = 1
#             trainOpenX, trainOpenY = create_dataset(numpy.reshape(train['open'].values, (train['open'].shape[0], 1)),
#                                                     look_back)
#             trainHighX, trainHighY = create_dataset(numpy.reshape(train['high'].values, (train['high'].shape[0], 1)),
#                                                     look_back)
#             trainLowX, trainLowY = create_dataset(numpy.reshape(train['low'].values, (train['low'].shape[0], 1)),
#                                                   look_back)
#             trainCloseX, trainCloseY = create_dataset(
#                 numpy.reshape(train['close'].values, (train['close'].shape[0], 1)),
#                 look_back)
#
#             testOpenX, testOpenY = create_dataset(numpy.reshape(test['open'].values, (test['open'].shape[0], 1)),
#                                                   look_back)
#             testHighX, testHighY = create_dataset(numpy.reshape(test['high'].values, (test['high'].shape[0], 1)),
#                                                   look_back)
#             testLowX, testLowY = create_dataset(numpy.reshape(test['low'].values, (test['low'].shape[0], 1)), look_back)
#             testCloseX, testCloseY = create_dataset(numpy.reshape(test['close'].values, (test['close'].shape[0], 1)),
#                                                     look_back)
#             # reshape input to be [samples, time steps, features]
#
#             trainOpenX = numpy.reshape(trainOpenX, (trainOpenX.shape[0], 1, trainOpenX.shape[1]))
#             trainHighX = numpy.reshape(trainHighX, (trainHighX.shape[0], 1, trainHighX.shape[1]))
#             trainLowX = numpy.reshape(trainLowX, (trainLowX.shape[0], 1, trainLowX.shape[1]))
#             trainCloseX = numpy.reshape(trainCloseX, (trainCloseX.shape[0], 1, trainCloseX.shape[1]))
#
#             testOpenX = numpy.reshape(testOpenX, (testOpenX.shape[0], 1, testOpenX.shape[1]))
#             testHighX = numpy.reshape(testHighX, (testHighX.shape[0], 1, testHighX.shape[1]))
#             testLowX = numpy.reshape(testLowX, (testLowX.shape[0], 1, testLowX.shape[1]))
#             testCloseX = numpy.reshape(testCloseX, (testCloseX.shape[0], 1, testCloseX.shape[1]))
#
#             # create and fit the LSTM network
#             model = Sequential()
#             model.add(LSTM(16, input_shape=(1, look_back)))
#             model.add(Dense(1))
#             model.compile(loss='mean_squared_error', optimizer='adam')
#             model.fit(trainOpenX, trainOpenY, epochs=10, batch_size=1, verbose=0)
#             trainOpenPredict = model.predict(trainOpenX)
#             testOpenPredict = model.predict(testOpenX)
#
#             model.fit(trainHighX, trainHighY, epochs=10, batch_size=1, verbose=0)
#             trainHighPredict = model.predict(trainHighX)
#             testHighPredict = model.predict(testHighX)
#
#             model.fit(trainLowX, trainLowY, epochs=10, batch_size=1, verbose=0)
#             trainLowPredict = model.predict(trainLowX)
#             testLowPredict = model.predict(testLowX)
#
#             model.fit(trainCloseX, trainCloseY, epochs=10, batch_size=1, verbose=0)
#             trainClosePredict = model.predict(trainCloseX)
#             testClosePredict = model.predict(testCloseX)
#
#             Train_m_Predict = numpy.concatenate(
#                 [trainOpenPredict, trainHighPredict, trainLowPredict, trainClosePredict],
#                 axis=1)
#             Test_m_Predict = numpy.concatenate([testOpenPredict, testHighPredict, testLowPredict, testClosePredict],
#                                                axis=1)
#
#             trainPredict = scaler.inverse_transform(Train_m_Predict)
#             testPredict = scaler.inverse_transform(Test_m_Predict)
#
#             df_trainP = pd.DataFrame(trainPredict, columns=dataset.columns)
#             df_testP = pd.DataFrame(testPredict, columns=dataset.columns)
#
#             frames = [df_trainP, df_testP]
#             finalSet = pd.concat(frames, ignore_index=True)
#             finalSet['date'] = dates
#
#             date_filter = (finalSet['date'].max()).split(' ')[0]
#
#             finalSet = finalSet[finalSet['date'].str.contains(date_filter)]
#
#             return finalSet.to_dict(orient='list')
#
#         except Exception as e:
#             print(str(e))
#             return str(e)
#
