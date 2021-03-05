import time
import numpy as np
import math
import random
import pymysql
from decimal import Decimal
from pandas import DataFrame
from openpyxl import Workbook
from sqlalchemy import create_engine


def spilt_data_by_layer(layer, data_df):
    return_df = []
    temp_df = DataFrame()
    for lay in layer:
        lay = list(lay)
        specimen = data_df.loc[(data_df['value'] <= max(lay)) & (data_df['value'] >= min(lay)), :]
        temp_df = temp_df.append(specimen, ignore_index=True)
        t_df = temp_df.copy(deep=True)
        return_df.append(t_df)
        temp_df.drop(temp_df.index, inplace=True)
        temp_df.columns = ['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                           'unit', 'latitude', 'longitude']
    return return_df


def sampling_all_data(engine, df_list, data_sum, sample_sum, database):
    denominator = 0
    lest = sample_sum
    molecular_list = []
    # 计算公式所需的每一层的分子分母
    for df in df_list:
        w = len(df) / data_sum
        desc = df.describe()
        std = round(float(desc.iloc[2]), 10)
        s = w * std
        if s < 0.0000000000000001:
            s = 0
        denominator = denominator + s
        molecular = sample_sum * w * std  # 分母 = 抽样量 * 该层数据占比 * 该层标准差
        molecular_list.append(molecular)
    # 利用公式计算出每层抽取样本数量进行抽样
    for molecular, df in zip(molecular_list, df_list):
        specimen = molecular / denominator
        sam = float(specimen) / len(df)
        if sam > 1:
            # print(df)
            # print(molecular, denominator, specimen)
            sam = 1
        if sam < 0 or sam is np.nan:
            sam = 0
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        # print(sam, len(df_sample))
        # print('-----------------------------------')
        lest = lest - len(df_sample)
        df_sample.to_sql(database, con=engine, if_exists='append', index=False, chunksize=100000)


def avg_sampling_all_data(engine, df_list, data_sum, sample_sum, database):
    denominator = 0
    lest = sample_sum
    molecular_list = []
    # 计算公式所需的每一层的分子分母
    for df in df_list:
        w = len(df) / data_sum
        desc = df.describe()
        std = round(float(desc.iloc[2]), 10)
        s = w * std
        if s < 0.0000000000000001:
            s = 0
        denominator = denominator + s
        molecular = sample_sum * w * std  # 分母 = 抽样量 * 该层数据占比 * 该层标准差
        molecular_list.append(molecular)
    # 利用公式计算出每层抽取样本数量进行抽样
    for molecular, df in zip(molecular_list, df_list):
        specimen = molecular / denominator  # 抽样数量
        df_length = len(df)
        sam = float(specimen) / df_length
        # 需要全部抽取
        if sam > 1:
            sam = 1
            df_sample = df.sample(frac=sam, replace=False, axis=0)
        else:
            try:
                hist = round(df_length / specimen)
            except:
                hist = round(sample_sum * (df_length / data_sum))
                if hist == 0:
                    hist = 1
            sam = 1 / hist
            df_sample = DataFrame([], columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter',
                                               'value', 'unit', 'latitude', 'longitude'])
            front = 0
            back = hist
            max_range = front + df_length - 1
            while front < max_range:
                data = df.loc[front:back, :]
                sample = data.sample(frac=sam, replace=False, axis=0)
                df_sample = df_sample.append(sample, ignore_index=True)
                front = back + 1
                back = back + hist
                if back > max_range:
                    back = max_range
        lest = lest - len(df_sample)
        df_sample.to_sql(database, con=engine, if_exists='append', index=False, chunksize=100000)
    # 抽取0项
