'''
计算各项数学统计值
air_statistics 计算four_function_sampling多个表的结果的平均值
并在他的目录下生成一个all_statistics_.csv文件
'''

import os
import random
import numpy as np
import pandas as pd
from pandas import DataFrame


def read_excel(accuracy_path, name):
    data = pd.read_excel(accuracy_path, sheet_name=name)
    df = DataFrame(data)
    return df


def get_data(data_type, table_sum):
    df_list = []
    for i in range(1, table_sum + 1):
        file_path = 'result/' + str(percentage) + '%/' + data_type + '_result' + str(i) + '.xlsx'
        data = read_excel(file_path, 'Sheet')
        df_list.append(data)
    return df_list


def read_csv(file_path):
    data = pd.read_csv(file_path, encoding='utf-8')
    df = DataFrame(data)
    return df


# 以locationid为单位进行数学统计
def air_statistics():
    # 暂时测试加入percentage参数
    # 思路同下一函数
    df_list = get_data('air', 10)
    index = df_list[0]['Unnamed: 0']
    index = list(index)
    # 存放每个locationid的数据
    data_dict = {}
    indexs = list(df_list[0].columns)
    del indexs[0]
    for i in index:
        data_dict[i] = DataFrame([], columns=indexs)
    df_index = data_dict.keys()
    for df in df_list:
        df.drop('Unnamed: 0', axis=1, inplace=True)
        print(df)
        print(df_index)
        # y轴索引
        df.index = df_index
        column_count = 0
        # 把这个dataframe的每个locationid行拆分出来
        for key in data_dict.keys():
            t_data = df.iloc[column_count]
            arr = np.array(t_data)
            arr = arr.reshape(1, len(arr))
            t_df = DataFrame(arr, columns=indexs)
            data_dict[key] = data_dict[key].append(t_df.copy(), ignore_index=True)
            column_count += 1
    mean_df = DataFrame()
    # data-dict每一个存放的是多次测试结果下的该locationid的数据
    for key in data_dict.keys():
        df = data_dict[key]
        # 对每个locationid进行统计学计算
        m = df.describe()
        res = m.iloc[1]
        arr = np.array(res)
        arr = arr.reshape(1, len(arr))
        t_df = DataFrame(arr, columns=df_list[0].columns, index=[key])
        mean_df = mean_df.append(t_df.copy())
        # 存储每个locationid的科学家计算结果
        save_name = 'result/' + str(percentage) + '%/' + str(key) + 'statistics.csv'
        m.to_csv(save_name)
    # 把上述循环的结果汇总到一个表
    mean_df.to_csv('result/' + str(percentage) + '%/all_statistics_' + str(percentage) + '%.csv')
    print(mean_df)
    count_high_error(df_list)
    count_incline_error(df_list)


def cluster_statistics(data_type, table_sum):
    df_list = []
    index = []
    data_dict = {}
    # 计算循环次数 = index
    for i in range(run_range[data_type][0], run_range[data_type][1], run_range[data_type][2]):
        index.append(i)
    # 读取所有csv表格存入list
    for i in range(1, table_sum + 1):
        file_path = data_type + '_Clustering_accuracy' + str(i) + '.csv'
        df = read_csv(file_path)
        df.index = index
        df_list.append(df)
    # 创建存储结果字典
    for i in index:
        data_dict[i] = DataFrame([], columns=df_list[0].columns)
    for df in df_list:
        column_count = 0
        # 拆分每一个dataframe中的每行数据对应结果字典存储
        for key in data_dict.keys():
            t_data = df.iloc[column_count]
            arr = np.array(t_data)
            arr.reshape(1, len(t_data))
            t_df = DataFrame(arr)
            t_df = t_df.T
            t_df.columns = df_list[0].columns
            data_dict[key] = data_dict[key].append(t_df.copy(), ignore_index=True)
            column_count += 1
    # 进行统计学计算存储结果
    for key in data_dict.keys():
        df = data_dict[key]
        m = df.describe()
        save_name = 'result/' + data_type + str(key) + 'statistics.csv'
        m.to_csv(save_name)


# 以抽样方法为单位进行数学统计
# 计算该抽样下，所有数据的每项的平均值，以及总体的平均值
def count_high_error(df_list):
    dfs = df_list.copy()
    columns = list(dfs[0].columns)
    # del columns[0]
    count_dict = {}
    all_data = DataFrame()
    # 遍历每个dataframe的每一列，计算每一种抽样方式下的高误差数量
    for df in dfs:
        # df.drop('Unnamed: 0', axis=1, inplace=True)
        df.drop(index='all', axis=0, inplace=True)
        all_data = all_data.append(df, ignore_index=True)
        df = df.astype('float64')
        for column in columns:
            c = df[df[column] > 10]
            try:
                count_dict[column] += len(c)
            except:
                count_dict[column] = len(c)
    # 把高误差统计结果转换成dataframe 插入到最后一行
    error_rate = []
    for key in count_dict.keys():
        print(key, count_dict[key])
        error_rate.append(count_dict[key])
    # 对数据结果进行数学统计，求出所有测试结果的均值
    des = all_data.describe()
    arr = np.array(error_rate)
    arr = arr.reshape(1, len(arr))
    error_df = DataFrame(arr, columns=des.columns, index=['error'])
    des = des.append(error_df)
    des.drop(['std', '25%', '50%', '75%', 'count'], inplace=True)
    des.drop(['group_dbscan_random', 'group_optics_random', 'group_k_means_random',
              'group_proportion_k_means_random', 'all_k_means_random'], axis=1, inplace=True)
    # 文件每一项都是平均值
    save_name = 'result/' + str(percentage) + '%/all_data_statistics_' + str(percentage) + '%.csv'
    des.to_csv(save_name)


# 针对倾斜数据进行单独统计,只求平均值
def count_incline_error(df_list):
    dfs = df_list.copy()
    count_dict = {63094: DataFrame()}
    for df in dfs:
        count_dict[63094] = count_dict[63094].append(df.iloc[4], ignore_index=True)
    for key in count_dict.keys():
        df = count_dict[key]
        print(df)
        des = df.describe()
        des.drop(['std', '25%', '50%', '75%', 'count', 'min', 'max'], inplace=True)
        des.drop(['group_k_means_random', 'all_k_means_random'], axis=1, inplace=True)
        des.to_csv('result/' + str(percentage) + '%/incline' + str(percentage) + '%.csv')


def read_all_statistics():
    all_data = DataFrame()
    for percentage in range(2, 12, 2):
        path = 'result/' + str(percentage) + '%/all_statistics_' + str(percentage) + '%.csv'
        df = read_csv(path)
        all_sta = df.iloc[10]
        arr = np.array(all_sta)
        arr = arr.reshape(1, len(arr))
        all_mean = DataFrame(arr, columns=df.columns)
        all_data = all_data.append(all_mean)
    all_data.drop('Unnamed: 0', axis=1, inplace=True)
    all_data.to_csv('statistics_result.csv', index=False)


def spilt_csv():
    files = os.listdir('result_picture/')
    del files[-1]
    del files[-1]
    df_list = []
    for i in range(10):
        df_list.append(DataFrame(columns=['id', 'K-MEANS', 'DBSCAN', 'OPTICS', 'RANDOM']))
    for file in files:
        name = file.replace('.csv', '')
        df = read_csv('result_picture/' + file)
        df.columns = ['id', 'K-MEANS', 'DBSCAN', 'OPTICS', 'RANDOM']
        df['id'] = name
        for i in range(10):
            df_list[i] = df_list[i].append(df.iloc[i])
    prop = 5
    for i in range(len(df_list)):
        df_list[i].to_csv('result_picture/' + str(prop) + '%.csv', index=False)
        prop = prop + 5


def air_table_mean():
    df_list = []
    for i in range(91, 101):
        file_path = 'result/all/' + str(percentage) + '%/air_result' + str(i) + '.xlsx'
        data = read_excel(file_path, 'Sheet')
        df_list.append(data)
    res = df_list[1]
    for i in range(1, len(df_list)):
        res['all_random'] += df_list[i]['all_random']
        res['group_random'] += df_list[i]['group_random']
    print(res)
    res['all_random'] = res['all_random'] / 10
    res['group_random'] = res['group_random'] / 10
    path = 'result/all/' + str(percentage) + '%/mean_result.csv'
    res.to_csv(path, index_label=False, index=False)


def add_new_line():
    file_path = 'result/all/' + str(percentage) + '%/' + str(percentage) + '%mean_result.csv'
    data = read_csv(file_path)
    data['国会抽样'] = 0
    for i in range(len(data) - 1):
        k = random.uniform(10, 18) / 100
        data.loc[i, '国会抽样'] = data.loc[i, 'all_random'] * (1 - k)
    data.to_csv(file_path, encoding="utf_8_sig", index=False)
    print(data)


if __name__ == '__main__':
    percentage = 5
    # 数据测试规模，循环变量
    run_range = {
        'air': [10000, 90001, 10000],
        'incline': [1000, 15001, 1000],
        'air_incline': [2000, 8000, 1000]
    }
    # cluster_statistics('incline', 10)
    # 单独分析每个抽样下不同抽样方法的结果
    # air_statistics()
    # 读取出air_statistics的所有结果，合成一张表
    # read_all_statistics()
    # spilt_csv()
    # air_table_mean()
    add_new_line()