import math
import sql_connect
import pandas as pd
from pandas import DataFrame


def read_csv(file_path):
    data = pd.read_csv(file_path)
    df = DataFrame(data)
    return df


if __name__ == '__main__':
    data = read_csv('_Clustering_accuracy.csv')
    data.loc[0] = abs(data.loc[0])
    data = data.sort_values(['0'])
    for i in data.itertuples():
        if abs(float(tuple(i)[2])) > 10:
            print(tuple(i))