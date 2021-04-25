import math
import sql_connect
import pandas as pd
from pandas import DataFrame

df = pd.read_excel('test.xlsx', sheet_name='Sheet1')
df.columns = ['score', 'id']
print(df)

print('-----------------')
t = df.sample(frac=0.6)
print(t)
print(t['score'].describe())