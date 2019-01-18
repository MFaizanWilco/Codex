import mongodb
import pandas as pd

def getData_intraDay(stockname='aapl'):
    value = mongodb.ReadValue('IntraDay', 'aapl')['Data']
    df = pd.DataFrame(eval(value),)
    # print(df)

    df.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low", "4. close": "close",
    "5. volume": "volume"}, inplace=True)
    #
    # if 'volume' in df.columns:
    #     del df['volume']

    dataframe = df.reset_index(drop=True)
    return dataframe


def get_dummyData():
    df = pd.read_csv('data')
    return df