import sql_connect
import numpy as np
from pandas import DataFrame
import cluster_sample_algorithm as csa


if __name__ == '__main__':
    sql_con = sql_connect.Sql_c()
    sql = 'select * from unknown_data.air;'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    data = DataFrame(result,
                     columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                              'unit', 'latitude', 'longitude', 'id'])
    data = data.drop(['id'], axis=1)
    data_sum = len(data)
    sql = 'select locationId, avg(value) from unknown_data.air group by locationId;'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    stand_res = {}
    # 计算标准结果
    for i in res:
        stand_res[i[0]] = float(i[1])
    res_dict = {}
    for i in range(10):
        sql_con.cursor.execute('TRUNCATE TABLE unknown_data.all_random;')
        sample_sum = int(data_sum * (5 / 100))
        df_sample = data.sample(frac=sample_sum / data_sum, replace=False, axis=0)
        df_sample.to_sql('all_random', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)
        sql = 'select locationId, avg(value) from unknown_data.all_random group by locationId;'
        sql_con.cursor.execute(sql)
        result = sql_con.cursor.fetchall()
        res_dict.clear()
        for i in result:
            try:
                stand = stand_res[i[0]]
            except:
                continue
            try:
                error = abs(stand - float(i[1])) / stand * 100
            except:
                error = abs(stand - float(i[1])) * 100
            res_dict[i[0]] = round(error, 6)
        accuracy_list = []
        keys = res_dict.keys()
        print(keys)
        key_list = []
        for key in keys:
            accuracy_list.append(res_dict[key])
            key_list.append(key)
        df = DataFrame(accuracy_list, index=key_list)
        print(df)
        df.to_csv(str(i) + 'accuracy.csv')