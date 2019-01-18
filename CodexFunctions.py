import numpy as np
import pandas as pd
import datasource as ds
from scipy.integrate import odeint
from pyti import simple_moving_average
from pyti import ultimate_oscillator
global vars
# from Depend import *
# import pytiDepends

vars = ["open", "high", "low", "close"]

def __init__(self):
    self.name = "CodexFunctionsApi"

def movingAvg(dataset, windowsize = 5):
    cap = 'movingAvg'
    if windowsize != 5:
        cap = cap + '_' + str(windowsize)
    dataset[cap] = 0
    dataset[cap] = dataset['close'].rolling(window=windowsize).mean()
    # dataset[cap+'P'] = dataset['closeP'].rolling(window=windowsize).mean()
    return dataset

def rolling_values(dataset, column, windowsize = 5):
    cap = 'roling' + '_' + column
    if windowsize != 5:
        cap = cap + '_' + str(windowsize)
    dataset[cap] = 0
    dataset[cap] = dataset[column].rolling(window=windowsize).mean()
    return dataset

def rolling_Std(dataset, windowsize = 5):
    cap = 'rolling_Std'
    if windowsize != 5:
        cap = cap + '_' + str(windowsize)
    dataset[cap] = 0
    dataset[cap] = dataset['open'].rolling(window=windowsize).std()
    return dataset

def cummulative_Avg(dataset, column):
    cap = 'cummulative_' + column
    dataset = dataset.dropna(axis=0)
    dataset = dataset.reset_index(drop=True)
    dataset['count'] = dataset.index + 1
    dataset[cap] = 0
    dataset[cap] = np.nancumsum(dataset[column])
    dataset[cap] = dataset[cap] / dataset['count']
    del dataset['count']
    return dataset

def cummulative_Sum(dataset,column):
    cap ='cummulativeSum'+column
    dataset[cap] = np.nancumsum(dataset[column])
    return dataset

def cummulative_Product(dataset,column):
    cap = 'cummulativeProduct' + column
    dataset[cap] = np.cumprod(dataset[column])
    return dataset

def variance(dataset,column):
    cap = 'Variance' + column
    dataset[cap] = np.var(dataset)
    return 0

def addUpperLower_UpDown(dataset):
    dataset['up or down'] = 0
    dataset['upper bound'] = 0.0
    dataset['lower bound'] = 0.0
    a = dataset.shape
    length = a[0] - 1
    for i in range(1, length):
        if ((dataset['high'].iat[i] - dataset['high'].iat[i - 1] > 0) & (
                dataset['high'].iat[i] - dataset['high'].iat[i + 1] > 0) & (
                dataset['low'].iat[i] - dataset['low'].iat[i - 1] > 0) & (
                dataset['low'].iat[i] - dataset['low'].iat[i + 1] > 0)):
            dataset['up or down'].iat[i] = 1
        if ((dataset['high'].iat[i] - dataset['high'].iat[i - 1] < 0) & (
                dataset['high'].iat[i] - dataset['high'].iat[i + 1] < 0) & (
                dataset['low'].iat[i] - dataset['low'].iat[i - 1] < 0) & (
                dataset['low'].iat[i] - dataset['low'].iat[i + 1] < 0)):
            dataset['up or down'].iat[i] = -1

    for i in range(3, length):
        if (dataset['up or down'].iat[i - 2] == 1):
            dataset['upper bound'].iat[i] = dataset['high'].iat[i - 2]
        else:
            dataset['upper bound'].iat[i] = dataset['upper bound'].iat[i - 1]

    for i in range(3, length):
        if (dataset['up or down'].iat[i - 2] == -1):
            dataset['lower bound'].iat[i] = dataset['low'].iat[i - 2]
        else:
            dataset['lower bound'].iat[i] = dataset['lower bound'].iat[i - 1]

    return dataset
# asset_log_returns = np.log(data).diff()
def addUpperLower_signal(dataset, num=15):
    dataset['upper signal'] = 0.0
    dataset['lower signal'] = 0.0
    a = dataset.shape
    for i in range(num, a[0] - num):
        max_value = 0.0
        min_value = 1000.0
        for j in range(num + 1):
            max_value = max(max_value, dataset['upper bound'].iat[i - j])
            min_value = min(min_value, dataset['lower bound'].iat[i - j])
        dataset['upper signal'].iat[i] = max_value
        dataset['lower signal'].iat[i] = min_value
    return dataset

def returns(dataset,column):
    cap = 'Returns_' + column
    dataset[cap] = np.log(dataset['close']/dataset['close'].shift(1))
    return dataset

def diffmodel(dx, dt):
    dydt=dx/dt
    return dydt

# def momentum(dataset):
#     cap='Momentum'
#     x = np.log(dataset['close'])
#     y0 = 5
#     t = np.linspace(0, 20)
#     t = dataset['date']
#     dataset[cap] = odeint(diffmodel(x,t),y0,t)
#     return dataset

def checkperiod(data,period):
    period = int(period)
    data_len = len(data)
    if data_len < period:
        raise Exception("Error: data_len < period")

def fill_for_noncomputable_vals(input_data, result_data):
    non_computable_values = np.repeat(
        np.nan, len(input_data) - len(result_data)
        )
    filled_result_data = np.append(non_computable_values, result_data)
    return filled_result_data

def rateofChange(dataset,period):
    return [dataset[idx] - dataset[idx + 1 - period] for idx in range(period - 1, len(dataset))]

def newrateofChange(dataset,period):
     a = pd.DataFrame([((dataset[idx] - dataset[idx - (period - 1)]) /dataset[idx - (period - 1)]) * 100 for idx in range(period - 1, len(dataset))])
     return a.round(5)


def momentum(dataset, period = 5):
    checkperiod(dataset, period)
    mom = rateofChange(dataset,period)
    mom = fill_for_noncomputable_vals(dataset, mom)
    return mom

def newmomentum(dataset,period):
    logP=np.log(dataset)
    rocs = newrateofChange(dataset,period)
    mom = fill_for_noncomputable_vals(dataset, rocs)
    return mom

def newthrust(dataset, period = 5):
    cap = 'Thrust'
    checkperiod(dataset, period)
    # dataset = dataset.dropna()
    y0 = [np.pi - 0.1, 0.0]
    dt = newrateofChange(dataset,period)
    dt = fill_for_noncomputable_vals(dataset, dt)
    t = np.linspace(0, 20)
    dataset = odeint(diffmodel(dataset,dt),y0,t)
    return dataset

def thrust(dataset,period = 5):
    rocs = newrateofChange(dataset,period)
    mom = fill_for_noncomputable_vals(dataset, rocs)
    return mom

def heavyThrust(dataset,period):
    rocs = newrateofChange(dataset, period)
    heavythru = fill_for_noncomputable_vals(dataset, rocs)
    return heavythru

def heavymomentum(dataset,period):
    logP = np.log(dataset['close'])
    logV = np.log(dataset['volume'])
    # dataset[cap] = odeint(diffmodel(dx,dt),y0,t)
    # dlogP=newrateofChange(logP,period)
    # dlogV = newrateofChange(logV, period)
    dataset['HeavyMomentum']= logP*logV
    dataset['HeavyMomentum'] = newrateofChange(dataset['HeavyMomentum'],period)
    return dataset['HeavyMomentum']

def CCI(dataset,n, constant):
    cap='CCI'
    TP = (dataset['high'] + dataset['low'] + dataset['close']) / 3
    dataset[cap] = pd.Series((TP - pd.rolling_mean(TP, n)) / (constant * pd.rolling_std(TP, n)), name='CCI_' + str(n))
    return dataset

# def simplemovingAvg(dataset):
#     cap='SimpleMovingAverage'
#     period = 6
#     sma = simple_moving_average.simple_moving_average(dataset, period)
#     np.testing.assert_array_equal(sma, setUp().sma_period_6_expected)
#     return dataset
#
# def ultimateOscillator(dataset):
#     bp = ultimate_oscillator.buying_pressure(dataset['close'], dataset['low'])
#     np.testing.assert_array_equal(bp, setUp().buying_pressure_expected)
#

#------------------------------------ Testing Code ------------------------------------

# Cf = functions()
#
# dataframe = ds.getData_intraDay('aapl')
# Cf.movingAvg(dataframe, 5)
# dataframe = Cf.cummulative_Avg(dataframe, 'close')
# dataframe = Cf.cummulative_Avg(dataframe, 'movingAvg')
# dataframe = Cf.cummulative_Sum(dataframe,'close')
# dataframe = Cf.cummulative_Product(dataframe,'close')
# dataframe = Cf.addUpperLower_UpDown(dataframe)
# dataframe = Cf.addUpperLower_signal(dataframe)
# dataframe = Cf.simplemovingAvg(dataframe)
# dataframe = Cf.returns(dataframe,'close')
# dataframe = Cf.momentum(dataframe)



