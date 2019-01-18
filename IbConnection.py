from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract
from threading import Thread
import queue
import datetime
import time
import datetime
from Config import Configuration
import mongodb
import pandas as pd


DEFAULT_HISTORIC_DATA_ID=1
DEFAULT_GET_CONTRACT_ID=43

## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()

class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we havew
                finished = True
                self.status = TIME_OUT


        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT

class TestWrapper(EWrapper):

    def __init__(self):
        self._my_contract_details = {}
        self._my_historic_data_dict = {}

    ## error handling code
    def init_error(self):
        error_queue=queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)


    ## get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        ## overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        ## overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)

    ## Historic data code
    def init_historicprices(self, tickerid):
        historic_data_queue = self._my_historic_data_dict[tickerid] = queue.Queue()

        return historic_data_queue


    def historicalData(self, tickerid , bar):

        ## Overriden method
        ## Note I'm choosing to ignore barCount, WAP and hasGaps but you could use them if you like
        bardata=(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)

        historic_data_dict=self._my_historic_data_dict

        ## Add on to the current data
        if tickerid not in historic_data_dict.keys():
            self.init_historicprices(tickerid)

        historic_data_dict[tickerid].put(bardata)

    def historicalDataEnd(self, tickerid, start:str, end:str):
        ## overriden method

        if tickerid not in self._my_historic_data_dict.keys():
            self.init_historicprices(tickerid)

        self._my_historic_data_dict[tickerid].put(FINISHED)

class TestClient(EClient):

    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)


    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID):

        """
        From a partially formed contract, returns a fully fledged version
        :returns fully resolved IB contract
        """

        ## Make a place to store the data we're going to return
        contract_details_queue = finishableQueue(self.init_contractdetails(reqId))

        print("Getting full contract details from the server... ")

        self.reqContractDetails(reqId, ibcontract)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        new_contract_details = contract_details_queue.get(timeout = MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if contract_details_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details)==0:
            print("Failed to get additional contract details: returning unresolved contract")
            return ibcontract

        if len(new_contract_details)>1:
            print("got multiple contracts using first one")

        new_contract_details=new_contract_details[0]

        resolved_ibcontract=new_contract_details.contract

        return resolved_ibcontract


    def get_IB_historical_data(self, ibcontract, durationStr="1 Y", barSizeSetting="1 day",
                               tickerid=DEFAULT_HISTORIC_DATA_ID):
        historic_data_queue = finishableQueue(self.init_historicprices(tickerid))

        self.reqHistoricalData(
            tickerid,  # tickerId,
            ibcontract,  # contract,
            datetime.datetime.today().strftime("%Y%m%d %H:%M:%S %Z"),  # endDateTime,
            durationStr,  # durationStr,
            barSizeSetting,  # barSizeSetting,
            "TRADES",  # whatToShow,
            1,  # useRTH,
            1,  # formatDate
            False,  # KeepUpToDate <<==== added for api 9.73.2
            [] ## chartoptions not used
        )



        ## Wait until we get a completed data, an error, or get bored waiting
        MAX_WAIT_SECONDS = 10
        print("Getting historical data from the server... could take %d seconds to complete " % MAX_WAIT_SECONDS)

        historic_data = historic_data_queue.get(timeout = MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if historic_data_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        self.cancelHistoricalData(tickerid)


        return historic_data

class TestApp(TestWrapper, TestClient):
    def __init__(self, ipaddress, portid, clientid):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target = self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.init_error()

class SaveIbApiData:
    def __init__(self):
        self.name = "DataFeed"
        self.Company = Configuration().GetData()['CompanyList']

    def Data_IntraDay(self,collectionname, Interval , data):
        # collectionname = 'IntraDay'
        for com in self.Company:
            try:
                mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
            except Exception as e:
                print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def Data_Daily(self,data):
        collectionname = 'Daily'
        for com in self.Company:
            try:
                mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
            except Exception as e:
                print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def Data_Weekly(self,data):
        collectionname = 'Weekly'
        for com in self.Company:
            try:
                mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
            except Exception as e:
                print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def Data_Monthly(self,com,data):
        collectionname = 'Monthly'
        try:
            mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
        except Exception as e:
            print('Company Ignore due to high service call' + '\nError : ' + str(e))

    def Data_Yearly(self,com,data):
        collectionname = 'Yearly'
        try:
            mongodb.UpdateValue(collectionname, com, data.to_dict(orient='list'))
        except Exception as e:
            print('Company Ignore due to high service call' + '\nError : ' + str(e))

    # def getNextDate(self, currrentDate):
    #     curDate = parser.parse(currrentDate).date()
    #     nextDay = curDate + timedelta(days=int(1))
    #     weekend = ['Saturday', 'Sunday']
    #
    #     while calendar.day_name[nextDay.weekday()] in weekend:
    #         nextDay = nextDay + timedelta(days=int(1))
    #     return nextDay

# def AddDataToMongo(com,durationstr,barSize,data):
#     if durationstr == "10 Y":
#         SaveIbApiData().Data_Yearly(com,data)
#         print('------------------------------ Daily Done ----------------------------------')
#     elif durationstr == "1 M":
#         SaveIbApiData().Data_Monthly(data)
#         print('----------------------------- Monthly Done ----------------------------------')
#     elif durationstr == "1 W":
#         SaveIbApiData().Data_Weekly(data)
#         time.sleep(120)
#         print('----------------------------- Weekly Done ----------------------------------')
#     elif durationstr == "1 D":
#         if barSize == "1 secs":
#             SaveIbApiData().Data_IntraDay("IntraDay_5s","1 secs",data)
#             print("'----------------------------- IntraDay_1s-1sec Done --------------------------------'")
#         elif barSize == "5 secs":
#             SaveIbApiData().Data_IntraDay("IntraDay_5s", "5 secs", data)
#             print('----------------------------- IntraDay_5s-5sec Done --------------------------------')
#         elif barSize == "15 secs":
#             SaveIbApiData().Data_IntraDay("IntraDay_15s", "15 secs", data)
#             print('----------------------------- IntraDay_15s-15sec Done --------------------------------')
#         elif barSize == "30 secs":
#             SaveIbApiData().Data_IntraDay("IntraDay_30s", "30 secs", data)
#             print('----------------------------- IntraDay_30s-30sec Done --------------------------------')
#         elif barSize == "1 mins":
#             SaveIbApiData().Data_IntraDay("IntraDay_1m", "1 min", data)
#             print('----------------------------- IntraDay_1m-1min Done --------------------------------')
#         elif barSize == "2 mins":
#             SaveIbApiData().Data_IntraDay("IntraDay_2m", "2 min", data)
#             print('----------------------------- IntraDay_2m-2min Done --------------------------------')
#         elif barSize == "3 mins":
#             SaveIbApiData().Data_IntraDay("IntraDay_3m", "3 min", data)
#             print('----------------------------- IntraDay_3m-3min Done --------------------------------')
#         elif barSize == "5 mins":
#             SaveIbApiData().Data_IntraDay("IntraDay_5m", "5 min", data)
#             print('----------------------------- IntraDay_5m-5min Done --------------------------------')
#         elif barSize == "15 mins":
#             SaveIbApiData().Data_IntraDay("IntraDay_15m", "15 min", data)
#             print('----------------------------- IntraDay_15m-15min Done --------------------------------')
#         elif barSize == "30 mins":
#             SaveIbApiData().Data_IntraDay("IntraDay_30m", "30 min", data)
#             print('----------------------------- IntraDay_30m-30min Done --------------------------------')
#         elif barSize == "1 hour":
#             SaveIbApiData().Data_IntraDay("IntraDay_1h", "1 hour", data)
#             print('----------------------------- IntraDay_1h-1hr Done --------------------------------')

def AddDataToMongo(com,barsize,data):
    # for barSize in range:
    if type(barsize) == '1 y':
        SaveIbApiData().Data_Yearly(com, data)
        print('------------------------------ Yearly Done ----------------------------------')
    if type(barsize) == '1 M':
        SaveIbApiData().Data_Monthly(com, data)
        print('------------------------------ Monthly Done ----------------------------------')
    if type(barsize) == '1 W':
        SaveIbApiData().Data_Weekly(com, data)
        print('------------------------------ Weekly Done ----------------------------------')
    if type(barsize) == '1 D':
        SaveIbApiData().Data_IntraDay(com, data)
        print('------------------------------ Daily Done ----------------------------------')
    if type(barsize) == '15 min':
        SaveIbApiData().Data_Monthly(com, data)
        print('------------------------------ IntraDay_15 Done ----------------------------------')
    if type(barsize) == '30 min':
        SaveIbApiData().Data_Monthly(com, data)
        print('------------------------------ IntraDay_30 Done ----------------------------------')
    if type(barsize) == '1 Hr':
        SaveIbApiData().Data_Monthly(com, data)
        print('------------------------------ IntraDay_60 Done ----------------------------------')

#
# def getData():
#

ibcontract = IBcontract()
ibcontract.secType = "STK"
# ibcontract.lastTradeDateOrContractMonth="201807"
ibcontract.symbol = "MSFT"
ibcontract.exchange = "SMART"


app = TestApp("127.0.0.1", 4002, 9)

resolved_ibcontract = app.resolve_ib_contract(ibcontract)
durationstr = "25 Y"
barSize = "1 M"

historic_data = app.get_IB_historical_data(resolved_ibcontract)
# livedata=app.reqMktData(DEFAULT_HISTORIC_DATA_ID,resolved_ibcontract,"mdoff,292:FLY+BRF",True,True,[])
# data=pd.DataFrame(livedata)

# historic_data = app.get_IB_historical_data(resolved_ibcontract,durationstr,barSize,DEFAULT_HISTORIC_DATA_ID)
# data = pd.DataFrame(historic_data)
print(historic_data.head())
print('----------Codex Now-----------')
print(len(historic_data))

# AddDataToMongo("MSFT", barSize, data)
AddDataToMongo("MSFT", barSize, historic_data)

app.disconnect()