import pandas as pd
import mongodb
value = mongodb.ReadValue('Daily', 'aapl')['Data']
df = pd.DataFrame(eval(value))
d=pd.DataFrame()
d=df.iloc[1]
d=df.iloc[2]
d=df.iloc[3]
print(d)
df= pd.DataFrame()
