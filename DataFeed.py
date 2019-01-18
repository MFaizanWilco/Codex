import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import schedule
import time
import sys
from alpha_vantage.timeseries import TimeSeries
from Config import Configuration
import mongodb
import pandas as pd
from dateutil import parser
from datetime import timedelta
import calendar
import stocksLSTM


class DataFeed:
    def __init__(self):
        self.name = "DataFeed"
        self.Company = Configuration().GetData()['CompanyList']
        self.CompanyP = Configuration().GetData()['CompanyListP']
        self.APIKEYS = Configuration().GetData()['APIKEYDICT']

    # to get ticket names (data we have) / Company_names
    def Feed_IntraDay(self, collectionname, Interval):
        # collectionname = 'IntraDay'
        for com in self.Company:
            try:
                ts = TimeSeries(key=self.APIKEYS[0], output_format='pandas')
                data, meta_data = ts.get_intraday(com, interval=Interval, outputsize="full")
                data = pd.DataFrame(data)
                data.reset_index(inplace=True)
                mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
                # mongodb.WriteValue(collectionname, com, data.to_dict(orient='list'))
                # print(data)
            except Exception as e:
                print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def Feed_Daily(self):
        collectionname = 'Daily'
        for com in self.Company:
            try:
                ts = TimeSeries(key=self.APIKEYS[5], output_format='pandas')
                data, meta_data = ts.get_daily(com, outputsize="full")
                data = pd.DataFrame(data)
                data.reset_index(inplace=True)
                mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
                # mongodb.WriteValue(collectionname, com, data.to_dict(orient='list'))
                # print(data)
            except Exception as e:
                print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def Feed_Weekly(self):
        collectionname = 'Weekly'
        for com in self.Company:
            try:
                ts = TimeSeries(key=self.APIKEYS[2], output_format='pandas')
                data, meta_data = ts.get_weekly(com)
                data = pd.DataFrame(data)
                data.reset_index(inplace=True)
                mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
                # mongodb.WriteValue(collectionname, com, data.to_dict(orient='list'))
                # print(data)
            except Exception as e:
                print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def Feed_Monthly(self):
        collectionname = 'Monthly'
        for com in self.Company:
            try:
                ts = TimeSeries(key=self.APIKEYS[3], output_format='pandas')
                data, meta_data = ts.get_monthly(com)
                data = pd.DataFrame(data)
                data.reset_index(inplace=True)
                mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
                # mongodb.WriteValue(collectionname, com, data.to_dict(orient='list'))
                # print(data)
            except Exception as e:
                print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def getNextDate(self, currrentDate):
        curDate = parser.parse(currrentDate).date()
        nextDay = curDate + timedelta(days=int(1))
        weekend = ['Saturday', 'Sunday']

        while calendar.day_name[nextDay.weekday()] in weekend:
            nextDay = nextDay + timedelta(days=int(1))
        return nextDay

    def next_day_prediction(self):
        collectionname = 'IntraDay'
        for com in self.CompanyP:
            value = mongodb.ReadValue(collectionname, com)['Data']
            df = pd.DataFrame(eval(value))
            # print(df)
            next_date = DataFeed().getNextDate((df['date'].max()).split(' ')[0])
            df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close",
                               "5. volume": "volume"}, inplace=True)

            if 'volume' in df.columns:
                del df['volume']
            dataframe = df.reset_index(drop=True)
            dates = dataframe['date'].copy()
            del dataframe['date']
            seedValue = dataframe.tail(1)
            dataframe, scaler = stocksLSTM.ScaleDataSet(dataframe)
            dataframe = stocksLSTM.prepareDataSet(dataframe)
            model, details = stocksLSTM.trainModel(dataframe)
            seedValue, _ = stocksLSTM.ScaleDataSet(seedValue, scaler)
            p_df = stocksLSTM.predictfulDay(model, details, seedValue)
            p_df = stocksLSTM.deScaleData(p_df, scaler)
            rng = pd.date_range(str(next_date) + ' ' + '09:35:00', periods=100, freq='5min')
            ts = pd.Series(rng)
            p_df['date'] = ts
            p_df['date'] = p_df['date'].astype(str)
            # print(p_df)
            mongodb.UpdateValue('FuturePrediction', com, p_df.to_dict(orient='list'))

    def same_day_prediction(self):
        collectionname = 'IntraDay'
        for com in self.CompanyP:
            value = mongodb.ReadValue(collectionname, com)['Data']
            df = pd.DataFrame(eval(value))
            # print(df)
            next_date = DataFeed().getNextDate((df['date'].max()).split(' ')[0])
            df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close",
                               "5. volume": "volume"}, inplace=True)
            if 'volume' in df.columns:
                del df['volume']
            dataframe = df.reset_index(drop=True)
            dates = dataframe['date'].copy()
            del dataframe['date']

            testEnd = dataframe.iloc[312:].copy()
            trainStart = dataframe.drop(dataframe.index[312:])

            trainStart, scaler = stocksLSTM.ScaleDataSet(trainStart)
            testEnd, _ = stocksLSTM.ScaleDataSet(testEnd, scaler)

            # testEnd = testEnd.shift(-1)
            # testEnd = testEnd.dropna()
            # testEnd.reset_index(drop=True, inplace=True)
            trainStart = stocksLSTM.prepareDataSet(trainStart)
            model, details = stocksLSTM.trainModel(trainStart)

            presults = stocksLSTM.predict(model, testEnd)
            presults = stocksLSTM.deScaleData(presults, scaler)
            ndates = pd.DataFrame(dates[312:], columns=['date'])
            # ndates = ndates.shift(-1)
            # ndates = ndates.dropna()
            ndates.reset_index(drop=True, inplace=True)
            presults = pd.concat([presults, ndates], axis=1)

            date_filter = (presults['date'].max()).split(' ')[0]
            mongodb.UpdateValue('PredictionStore', com + ' ' + str(date_filter), presults.to_dict(orient='list'))


def send_email():
    sender = Configuration().GetData()['EmailID']
    gmail_password = Configuration().GetData()['Password']
    COMMASPACE = ', '
    recipients = ['mfaizan@codexnow.com']

    # Create the enclosing (outer) message
    outer = MIMEMultipart()
    outer['Subject'] = 'DataFeed @ ' + str(datetime.datetime.now().date())
    outer['To'] = COMMASPACE.join(recipients)
    outer['From'] = sender
    outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'

    msg = MIMEText('Data Feeding Start in Mongodb' + str(datetime.datetime.now()))
    outer.attach(msg)
    composed = outer.as_string()

    # Send the email
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(sender, gmail_password)
            s.sendmail(sender, recipients, composed)
            s.close()
            print("Email sent!")
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])
        raise


def running_data_feed():
    # send_email()
    DataFeed().Feed_IntraDay("IntraDay", "5min")
    print('----------------------------- IntraDay-5min Done --------------------------------')
    time.sleep(60)
    DataFeed().Feed_IntraDay("IntraDay_15", "15min")
    print('----------------------------- IntraDay-15min Done --------------------------------')
    time.sleep(120)
    DataFeed().Feed_IntraDay("IntraDay_30", "30min")
    print('----------------------------- IntraDay-30min Done --------------------------------')
    time.sleep(60)
    DataFeed().Feed_IntraDay("IntraDay_60", "60min")
    print('----------------------------- IntraDay-60min Done --------------------------------')
    time.sleep(60)
    DataFeed().Feed_Daily()
    print('------------------------------ Daily Done ----------------------------------')
    time.sleep(60)
    DataFeed().Feed_Weekly()
    print('----------------------------- Weekly Done ----------------------------------')
    time.sleep(60)
    DataFeed().Feed_Monthly()
    print('----------------------------- Monthly Done ----------------------------------')
    DataFeed().same_day_prediction()
    DataFeed().next_day_prediction()


def daily_feeding():
    print("Run-----")
    schedule.every().day.at("09:00").do(running_data_feed)
    while True:
        schedule.run_pending()
        time.sleep(60)     # wait one minute


# daily_feeding()  catch_errors.check_for_period_error(data, period)
#
#     momentum = [data[idx] - data[idx+1-period] for idx in range(period-1, len(data))]
#     momentum = fill_for_noncomputable_vals(data, momentum)
#     return momentum
running_data_feed()
# DataFeed().save_prediction()

# DataFeed().same_day_prediction()
# DataFeed().next_day_prediction()
