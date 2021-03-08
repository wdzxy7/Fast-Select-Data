import pandas as pd
import numpy as np
from pandas import DataFrame


def read_excel(accuracy_path, name):
    data = pd.read_excel(accuracy_path, sheet_name=name)
    df = DataFrame(data)
    return df


def get_data(data_type, table_sum):
    df_list = []
    for i in range(1, table_sum + 1):
        file_path = data_type + '_result' + str(i) + '.xlsx'
        data = read_excel(file_path, 'Sheet')
        df_list.append(data)
    return df_list


def read_csv(file_path):
    data = pd.read_csv(file_path)
    df = DataFrame(data)
    return df


def air_statistics():
    df_list = get_data('air', 10)
    index = df_list[0]['Unnamed: 0']
    index = list(index)
    data_dict = {}
    for i in index:
        column = list(df_list[0].columns)
        del column[0]
        data_dict[i] = DataFrame([], columns=column)
    df_index = data_dict.keys()
    for df in df_list:
        df.drop('Unnamed: 0', axis=1, inplace=True)
        df.index = df_index
        column_count = 0
        for key in data_dict.keys():
            t_data = df.iloc[column_count]
            arr = np.array(t_data)
            arr = arr.reshape(1, 15)
            t_df = DataFrame(arr, columns=column)
            data_dict[key] = data_dict[key].append(t_df.copy(), ignore_index=True)
            column_count += 1
    for key in data_dict.keys():
        df = data_dict[key]
        m = df.describe()
        save_name = str(key) + 'statistics.csv'
        m.to_csv(save_name)


def cluster_statistics(data_type, table_sum):
    df_list = []
    index = []
    data_dict = {}
    for i in range(run_range[data_type][0], run_range[data_type][1], run_range[data_type][2]):
        index.append(i)
    for i in range(1, table_sum + 1):
        file_path = data_type + '_Clustering_accuracy' + str(i) + '.csv'
        df = read_csv(file_path)
        df.index = index
        df_list.append(df)
    for i in index:
        data_dict[i] = DataFrame([], columns=df_list[0].columns)
    for df in df_list:
        column_count = 0
        for key in data_dict.keys():
            t_data = df.iloc[column_count]
            arr = np.array(t_data)
            arr.reshape(1, len(t_data))
            t_df = DataFrame(arr)
            t_df = t_df.T
            t_df.columns = df_list[0].columns
            data_dict[key] = data_dict[key].append(t_df.copy(), ignore_index=True)
            column_count += 1
    for key in data_dict.keys():
        df = data_dict[key]
        m = df.describe()
        save_name = 'result/' + data_type + str(key) + 'statistics.csv'
        m.to_csv(save_name)


if __name__ == '__main__':
    run_range = {
        'air': [10000, 90001, 10000],
        'incline': [1000, 15001, 1000],
        'air_incline': [2000, 8000, 1000]
    }
    cluster_statistics('incline', 10)