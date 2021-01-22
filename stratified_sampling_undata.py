from decimal import Decimal
import pymysql
from pandas import Series, DataFrame
import time
from sqlalchemy import create_engine
import pandas as pd
from pandas.api.types import CategoricalDtype
import fsspec


if __name__ == '__main__':
    sample = 12000
    store_df = DataFrame([], columns=['id', 'index', 'time', 'score'])
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 清空表
    sql = 'TRUNCATE TABLE unknown_data.test_sample;'
    cursor.execute(sql)
    sql = 'select * from unknown_data.data3;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df = DataFrame(result, columns=['id', 'index', 'time', 'score'])
    index = df.loc[:, 'index']
    index = index.drop_duplicates()
    avg = sample / len(index)  # 平均每个index抽样多 少
    more_index = []  # 记录该列数据数量大于avg的index编号
    print(1)
    count = 1
    for i in index:
        print(i, len(index), count)
        count = count + 1
        t = df.loc[df['index'] == i]
        if len(t) > avg:
            more_index.append(t)
        else:
            store_df = store_df.append(t, ignore_index=True)
    print(2)
    avg = (sample - len(store_df)) / len(more_index)  # 计算剩余部分每个抽多少
    for t in more_index:
        df_sample = t.sample(frac=avg / len(t), replace=False, axis=0)
        store_df = store_df.append(df_sample, ignore_index=True)
    print(3, len(store_df))
    store_df.to_sql('test_sample', con=engine, if_exists='append', index=False, chunksize=100000)