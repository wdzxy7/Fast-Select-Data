'''
该代码是对air数据集做个全部的测试
处理数据维air中的所有数据。
一种是random即全部数据拿来直接按照比率全体抽样
group 是按照id分组，然后每组根据组内数量占总体的占比进行抽样
'''
import time
import sql_connect
import numpy as np
import pandas as pd
from pandas import DataFrame
from sklearn.cluster import KMeans
import four_function_sampling as ffs
import cluster_sample_algorithm as csa


def random_test():
    sql_con = sql_connect.Sql_c()
    sql = 'select * from unknown_data.air;'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    print(1)
    data = DataFrame(result,
                     columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                              'unit', 'latitude', 'longitude', 'id'])
    data = data.drop(['id'], axis=1)
    data_sum = len(data)
    sql = 'select locationId, avg(value) from unknown_data.air group by locationId;'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    print(2)
    stand_res = {}
    all_df = DataFrame()
    # 计算标准结果
    for i in res:
        stand_res[i[0]] = float(i[1])
    for i in range(10):
        sql_con.cursor.execute('TRUNCATE TABLE unknown_data.all_random;')
        sample_sum = int(data_sum * (10 / 100))
        df_sample = data.sample(frac=sample_sum / data_sum, replace=False, axis=0)
        df_sample.to_sql('all_random', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)
        sql = 'select locationId, avg(value) from unknown_data.all_random group by locationId;'
        sql_con.cursor.execute(sql)
        result = sql_con.cursor.fetchall()
        accuracy_list = []
        key_list = []
        for res in result:
            try:
                stand = stand_res[res[0]]
            except:
                continue
            try:
                error = abs(stand - float(res[1])) / stand * 100
            except:
                error = abs(stand - float(res[1])) * 100
            accuracy_list.append(round(error, 6))
            key_list.append(res[0])
        df = DataFrame(accuracy_list, index=key_list)
        if i == 0:
            all_df = df
        else:
            all_df = all_df + df
        print(df)
    all_df = all_df / 10
    all_df.to_csv('_Clustering_accuracy10.csv')


def group_test():
    # 测试sql
    t_sql1 = 'select avg(score) from unknown_data.proportion_k_means_result;'
    # t_sql2 = 'select avg(score) from unknown_data.dbscan_result;'
    # t_sql3 = 'select avg(score) from unknown_data.optics_result;'
    t_sql2 = 'select avg(score) from unknown_data.proportion_dbscan_result;'
    t_sql3 = 'select avg(score) from unknown_data.proportion_optics_result;'
    test_sql = [t_sql1, t_sql2, t_sql3]
    # 清表sql
    # sql1 = 'TRUNCATE TABLE unknown_data.optics_result;'
    # sql2 = 'TRUNCATE TABLE unknown_data.dbscan_result;'
    sql1 = 'TRUNCATE TABLE unknown_data.proportion_optics_result;'
    sql2 = 'TRUNCATE TABLE unknown_data.proportion_dbscan_result;'
    sql3 = 'TRUNCATE TABLE unknown_data.proportion_k_means_result;'
    clear_sql = [sql1, sql2, sql3]
    sql_con = sql_connect.Sql_c()
    data = pd.read_excel('parameter.xlsx', sheet_name='Sheet1')
    df = DataFrame(data).astype('float')
    group_dict = {}
    for i in df.itertuples():
        t = list(i)
        group_dict[int(t[1])] = [t[2], int(t[3]), t[4]]
    result_list = []
    result_df = DataFrame(columns=['K-MEANS', 'DBSCAN', 'OPTICS'])
    all_time = [0, 0, 0]
    for key in group_dict.keys():
        result_list.clear()
        # 查询该group数据
        t1 = time.perf_counter()
        select_sql = 'select value from unknown_data.air where locationId=' + str(key) + ';'
        sql_con.cursor.execute(select_sql)
        result = sql_con.cursor.fetchall()
        group_data = DataFrame(result,
                               columns=['score'])
        # 查询相同数据数量 为聚类运算准备
        same_sql = 'select avg(value), count(value) from unknown_data.air where locationId=' + str(key) + ' group by value;'
        sql_con.cursor.execute(same_sql)
        result = sql_con.cursor.fetchall()
        t2 = time.perf_counter()
        select_time = t2 - t1
        same_data = {}
        for i in result:
            same_data[float(i[0])] = int(i[1])
        arr = group_data['score']
        values = []
        for i in arr:
            values.append((i, 0))
        arr = np.array(values)
        print(group_dict[key])
        Eps = group_dict[key][0]
        MinPts = group_dict[key][1]
        kt1 = time.perf_counter()
        cluster = KMeans(n_clusters=3).fit(arr)
        k_means_layer = ffs.get_cluster(cluster, values)
        kmdata = csa.spilt_data_by_layer(k_means_layer, group_data)
        kt2 = time.perf_counter()
        k_time = ((kt2 - kt1) + select_time) * 10
        opt1 = time.perf_counter()
        optics_layer = csa.OPTICS(same_data, Eps=Eps, MinPts=MinPts)
        opdata = csa.spilt_data_by_layer(optics_layer, group_data)
        opt2 = time.perf_counter()
        op_time = ((opt2 - opt1) + select_time) * 10
        dbt1 = time.perf_counter()
        dbscan_layer = csa.DBSCAN(same_data, Eps=Eps, MinPts=MinPts)
        dbdata = csa.spilt_data_by_layer(dbscan_layer, group_data)
        dbt2 = time.perf_counter()
        db_time = ((dbt2 - dbt1) + select_time) * 10
        stand = group_dict[key][2]
        data_sum = len(group_data)
        t_result = []
        for j in range(10):
            t_result.clear()
            sample_sum = int(data_sum * (5 / 100))
            for sql in clear_sql:
                sql_con.cursor.execute(sql)
            kt1 = time.perf_counter()
            csa.proportion_sample_data(sql_con.engine, kmdata, data_sum, sample_sum, 'proportion_k_means_result')
            kt2 = time.perf_counter()
            k_time = k_time + (kt2 - kt1)
            dbt1 = time.perf_counter()
            csa.sampling_data(sql_con.engine, dbdata, data_sum, sample_sum, 'proportion_dbscan_result')
            dbt2 = time.perf_counter()
            db_time = db_time + (dbt2 - dbt1)
            opt1 = time.perf_counter()
            csa.sampling_data(sql_con.engine, opdata, data_sum, sample_sum, 'proportion_optics_result')
            opt2 = time.perf_counter()
            op_time = op_time + (opt2 - opt1)
            for test in test_sql:
                sql_con.cursor.execute(test)
                result = sql_con.cursor.fetchall()
                avg = round(float(result[0][0]), 6)
                accuracy = round(abs(avg - stand) / abs(stand) * 100, 6)
                t_result.append(accuracy)
            t = t_result.copy()
            result_list.append(t)
        # print(result_list, end='\n------------------------------')
        df = DataFrame(result_list, columns=['K-MEANS', 'DBSCAN', 'OPTICS'])
        # print(df)
        des = df.describe()
        means = des.iloc[1]
        print(means)
        result_df = result_df.append(means, ignore_index=True)
        all_time[0] += k_time
        all_time[1] += db_time
        all_time[2] += k_time
    k_time = all_time[0] / 10
    db_time = all_time[1] / 10
    op_time = all_time[2] / 10
    print(round(k_time, 6), round(op_time, 6), round(db_time, 6))
    result_df.index = list(group_dict.keys())
    print(result_df)
    result_df.to_csv('result2.csv')


if __name__ == '__main__':
    group_test()