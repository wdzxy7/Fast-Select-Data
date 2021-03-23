import pymysql
from pandas import Series, DataFrame
import time
from sqlalchemy import create_engine
import pandas as pd
from pandas.api.types import CategoricalDtype
import fsspec


if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()

    sql = 'select * from unknown_data.air;'
    df = pd.read_sql(sql, con=engine)
    for proportion in range(1, 4):
        sql = 'TRUNCATE TABLE unknown_data.small_test;'
        cursor.execute(sql)
        df_sample = df.sample(frac=proportion / 100, replace=False, axis=0)
        df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)
        sql = 'SELECT country,parameter, avg(`value`) FROM unknown_data.air GROUP BY country, parameter;'
        cursor.execute(sql)
        res = cursor.fetchall()
        stand_res = {}
        # 计算标准结果
        for i in res:
            stand_res[(i[0], i[1])] = float(i[2])
        sql = 'SELECT country,parameter, avg(`value`) FROM unknown_data.small_test GROUP BY country, parameter;'
        cursor.execute(sql)
        res = cursor.fetchall()
        sample_result = {}
        for i in res:
            sample_result[(i[0], i[1])] = float(i[2])
        for key in sample_result.keys():
            stand = stand_res[key]
            test = sample_result[key]
            error = abs(stand - test) / stand * 100
            print(key, error)
        print('----------------------------------------------')