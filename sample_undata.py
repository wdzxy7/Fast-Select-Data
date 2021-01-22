from decimal import Decimal
import pymysql
from pandas import Series, DataFrame
import time
from sqlalchemy import create_engine
import pandas as pd
from pandas.api.types import CategoricalDtype
import fsspec


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 清空表
    sql = 'TRUNCATE TABLE unknown_data.test_sample;'
    cursor.execute(sql)
    time.process_time()
    # 查询总数据
    sql = 'select * from unknown_data.data1;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df = DataFrame(result, columns=['id', 'index', 'time', 'score'])
    # 抽样
    t1 = time.perf_counter()
    df_sample = df.sample(frac=0.00125, replace=False, axis=0)
    store_list = []
    for mes in df_sample.itertuples():
        dt = list(mes)
        del dt[0]
        dt[2] = str(dt[2])
        store_list.append(tuple(dt))
    sql = 'INSERT INTO unknown_data.test_sample (id, `index`, time, score) VALUES (%s, %s, %s, %s);'
    print(sql)
    print(store_list)
    cursor.executemany(sql, store_list)