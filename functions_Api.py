import numpy as np
import pandas as pd
import datasource as ds
from scipy.integrate import odeint
from pyti import simple_moving_average
from pyti import ultimate_oscillator
global vars
from Depend import *

class functions(Resource):
    vars = ["open", "high", "low", "close"]

    def __init__(self):
        self.name = "CodexFunctionsApi"
        # self.display_data = display_data()
        self.Company = Configuration().GetData()['CompanyList']

    def setUp(self):
        sma_period_6_expected = [np.nan, np.nan, np.nan, np.nan, np.nan,
                                      804.55166666666673, 807.84333333333336, 809.89666666666665,
                                      811.21833333333325, 811.20333333333338, 812.51166666666666,
                                      813.88000000000011, 814.40333333333331, 813.18666666666661,
                                      812.6783333333334, 810.23333333333346, 806.20333333333338,
                                      799.25166666666667, 793.06499999999994, 785.82499999999993,
                                      778.30499999999995, 775.09000000000003, 774.75166666666667,
                                      776.35333333333347, 776.68333333333339, 779.10666666666668,
                                      782.55166666666673, 784.03833333333341, 781.79333333333341,
                                      781.85500000000002, 781.81833333333327, 781.17833333333328,
                                      775.88166666666666, 773.70666666666659, 774.42666666666662,
                                      777.66499999999996, 782.99833333333333, 787.4766666666668,
                                      792.12333333333345, 793.86333333333334, 795.21833333333336,
                                      795.20000000000016, 794.85333333333335, 797.77499999999998,
                                      803.81666666666672, 810.46833333333336, 817.15666666666664,
                                      822.19999999999993, 824.55999999999983, 824.90499999999986,
                                      826.52833333333331, 826.42666666666662, 822.80833333333339,
                                      817.61833333333345, 814.28833333333341, 812.64499999999998,
                                      809.72499999999991, 808.505, 807.48333333333323, 807.23000000000002,
                                      806.75500000000011, 805.25833333333321, 803.72666666666657,
                                      802.04166666666663, 802.36333333333334, 803.52666666666664,
                                      805.11000000000001, 805.08666666666659, 807.51666666666677,
                                      809.49833333333333, 809.89666666666665, 808.18333333333328,
                                      805.62666666666667, 804.84666666666669, 802.55833333333339,
                                      798.31000000000006, 795.5916666666667, 795.43166666666673,
                                      794.28000000000009, 795.0916666666667, 796.21833333333336,
                                      799.1450000000001, 800.50333333333344, 799.26666666666677, 799.495,
                                      797.67500000000007, 795.64666666666665, 793.17999999999995,
                                      792.25166666666667, 792.61833333333345, 793.74166666666667,
                                      794.58000000000004, 795.21833333333325, 796.80666666666673,
                                      799.15999999999997, 800.42500000000007, 801.98666666666668,
                                      803.67000000000007, 805.09499999999991, 806.05166666666662,
                                      806.39499999999987, 807.06833333333327, 807.23000000000002,
                                      805.59666666666669, 804.04999999999984, 802.65500000000009,
                                      801.56499999999994, 799.25, 792.40166666666664, 786.52166666666665,
                                      779.64333333333332, 772.54333333333341, 765.60000000000002,
                                      759.44500000000005, 757.98500000000001, 756.55833333333328,
                                      755.81666666666661, 752.16833333333341, 748.25500000000011,
                                      744.10000000000002, 740.005, 735.63666666666666, 729.73333333333323,
                                      725.005, 720.53333333333342, 716.43500000000006, 712.72500000000002]

        buying_pressure_expected = [np.nan, 10.42999999999995,
                                         1.6900000000000546, 5.3599999999999, 0.0, 1.8799999999999955,
                                         2.5200000000000955, 3.0, 0.0, 0.0, 5.8099999999999454,
                                         2.2400000000000091, 0.0, 0.0, 1.5500000000000682, 0.0, 0.0, 0.0,
                                         2.0400000000000773, 0.0, 0.0, 11.549999999999955, 13.560000000000059,
                                         0.0, 0.0, 0.0, 6.0, 0.0, 0.0, 10.189999999999941, 0.0, 0.0, 0.0,
                                         18.529999999999973, 8.5399999999999636, 25.300000000000068,
                                         6.3899999999999864, 0.0, 0.0, 1.0900000000000318, 6.2299999999999045,
                                         17.060000000000059, 4.4199999999999591, 9.6599999999999682, 0.0, 4.75,
                                         6.4499999999999318, 7.1900000000000546, 0.0, 0.0, 5.4600000000000364,
                                         0.0, 0.0, 0.0, 0.0, 7.6899999999999409, 0.0, 4.5999999999999091, 0.0,
                                         2.3700000000000045, 0.0, 1.5599999999999454, 0.0, 3.67999999999995, 0.0,
                                         7.4199999999999591, 0.67000000000007276, 0.0, 12.310000000000059,
                                         0.99000000000000909, 0.0, 0.0, 0.0, 2.5800000000000409,
                                         3.2599999999999909, 0.0, 0.0, 10.100000000000023, 0.0,
                                         14.360000000000014, 5.1499999999999773, 0.029999999999972715, 0.0, 0.0,
                                         0.0, 2.0699999999999363, 3.9000000000000909, 0.0, 0.0,
                                         2.3000000000000682, 2.9900000000000091, 0.36000000000001364,
                                         2.6999999999999318, 3.1000000000000227, 2.6699999999999591, 0.0,
                                         4.7699999999999818, 1.0899999999999181, 1.1500000000000909,
                                         0.28999999999996362, 0.0, 0.0, 1.6999999999999318, 0.0,
                                         1.6699999999999591, 1.2000000000000455, 0.82000000000005002, 0.0, 0.0,
                                         0.0, 0.0, 0.0, 1.7599999999999909, 0.0, 2.6700000000000728, 0.0, 0.0,
                                         0.0, 0.16999999999995907, 0.0, 3.0299999999999727, 0.0, 0.0, 0.0,
                                         1.7100000000000364, 0.0, 5.3600000000000136]
    @jwt_required
    def movingAvg(dataset, windowsize = 5):
        cap = 'movingAvg'
        if windowsize != 5:
            cap = cap + '_' + str(windowsize)
        dataset[cap] = 0
        dataset[cap] = dataset['close'].rolling(window=windowsize).mean()
        return dataset

    @jwt_required
    def rolling_values(dataset, column, windowsize = 5):
        cap = 'roling' + '_' + column
        if windowsize != 5:
            cap = cap + '_' + str(windowsize)
        dataset[cap] = 0
        dataset[cap] = dataset[column].rolling(window=windowsize).mean()
        return dataset

    @jwt_required
    def rolling_Std(dataset, windowsize = 5):
        cap = 'rolling_Std'
        if windowsize != 5:
            cap = cap + '_' + str(windowsize)
        dataset[cap] = 0
        dataset[cap] = dataset['open'].rolling(window=windowsize).std()
        return dataset

    @jwt_required
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
        cap ='cummulativeProduct'+column
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

    def heavymomentum(dataset):
        cap = 'HeavyMomentum'
        # HeavyMomentum = (np.log(dataset['close'])*np.log(dataset['volume']))/dataset['datetime']
        # diffrential k sath Momentum
        dx=(np.log(dataset['close'])*np.log(dataset['volume']))
        dt= dataset['datetime']
        y0 = 5
        t = np.linspace(0,20)
        # t=dataset['datetime']
        dataset[cap] = odeint(function.diffmodel,y0,t)
        # by the method of Web
        dataset = dataset.reset_index(drop=True)
        dataset[cap] = dataset['close'] - dataset['close'].index - 10
        return dataset

    def momentum(dataset):
        cap='Momentum'
        x = np.log(dataset['close'])
        y0 = 5
        t = dataset['datetime']
        dataset[cap]=odeint(functions.diffmodel(x),y0,t)
        return dataset

    def heavyThrust(dataset):
        cap='HeavyThrust'
        x=np.log(dataset['close'])
        y0=5
        t=dataset['datetime']
        dataset[cap]=odeint(functions.diffmodel(x),y0,t)
        return dataset

    def thrust(dataset):
        cap='Thrust'
        x=dataset['Momentum']
        y0=5
        t=dataset['datetime']
        dataset[cap]=odeint(functions.diffmodel(x),y0,t)
        return dataset

    def CCI(dataset,n, constant):
        cap='CCI'
        TP = (dataset['high'] + dataset['low'] + dataset['close']) / 3
        dataset[cap] = pd.Series((TP - pd.rolling_mean(TP, n)) / (constant * pd.rolling_std(TP, n)), name='CCI_' + str(n))
        return dataset

    def simplemovingAvg(dataset):
        cap='SimpleMovingAverage'
        period = 6
        sma = simple_moving_average.simple_moving_average(dataset, period)
        np.testing.assert_array_equal(sma, functions.setUp().sma_period_6_expected)
        return dataset

    def ultimateOscillator(dataset):
        bp = ultimate_oscillator.buying_pressure(dataset['close'], dataset['low'])
        np.testing.assert_array_equal(bp, functions.setUp().buying_pressure_expected)


#------------------------------------ Testing Code ------------------------------------

Cf = functions()

dataframe = ds.getData_intraDay('aapl')
Cf.movingAvg(dataframe, 5)
dataframe = Cf.cummulative_Avg(dataframe, 'close')
dataframe = Cf.cummulative_Avg(dataframe, 'movingAvg')
dataframe = Cf.cummulative_Sum(dataframe,'close')
dataframe = Cf.cummulative_Product(dataframe,'close')
dataframe = Cf.addUpperLower_UpDown(dataframe)
dataframe = Cf.addUpperLower_signal(dataframe)
dataframe = Cf.simplemovingAvg(dataframe)
dataframe = Cf.returns(dataframe,'close')
dataframe = Cf.momentum(dataframe)



