import time
import pymysql
from pandas import DataFrame
from decimal import Decimal
import pandas as pd
import single_test as st
from sqlalchemy import create_engine


def create_data_test():
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 抽样
    columns_list = ['biological', 'math', 'music', 'political', 'chemistry', 'chinese', 'geography', 'history',
                    'Line_generation', 'art', 'english', 'physics']
    # 设置第一行的标准数据 
    first_data = [[400.2163, 400.0873, 400.3409, 400.0527, 400.0152, 400.4353, 400.0475, 400.0209, 400.1792, 399.8196,
                   399.9599, 399.1788]]
    df = DataFrame(first_data, columns=columns_list)
    print(df)
    for sample_num in range(1, 21):
        for term_num in range(1, 4):
            sql = 'Select major,avg(score) from grades.stratified_sample' + str(sample_num) + ' where term = ' + str(term_num) + ' group by major;'
            print(sql)
            cursor.execute(sql)
            result = cursor.fetchall()
            for re in result:
                write_column = re[0]
                write_data = re[1]
                df.loc[int(sample_num), write_column] = write_data
    print(df)
    ind = []
    sam = Decimal('0.005')
    c = Decimal('0.005')
    for i in range(20):
        ind.append(str(sam) + '%')
        sam = sam + c
    ind.insert(0, '100%')
    df.index = ind
    print(df)
    df.to_csv('accuracy.csv')


def avg_sampling(df, sample_sum, engine):
    front = 0
    back = 0
    data_sum = len(df)
    max_range = front + data_sum - 1
    hist = int(data_sum / sample_sum)
    sam = 1 / hist
    store_df = DataFrame([], columns=['score']).astype('float')
    while front < max_range:
        data = df.loc[front:back, :]
        df_sample = data.sample(frac=sam, replace=False, axis=0)
        store_df = store_df.append(df_sample, ignore_index=True)
        front = back + 1
        back = back + hist
        if back > max_range:
            back = max_range
    store_df.to_sql('avg_result', con=engine, if_exists='append', index=False, chunksize=100000)


def real_data_test():
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 测试sql
    t_sql1 = 'select avg(score) from unknown_data.k_means_result;'
    t_sql2 = 'select avg(score) from unknown_data.avg_k_means_result;'
    t_sql3 = 'select avg(score) from unknown_data.dbscan_result;'
    t_sql4 = 'select avg(score) from unknown_data.avg_dbscan_result;'
    t_sql5 = 'select avg(score) from unknown_data.optics_result;'
    t_sql6 = 'select avg(score) from unknown_data.avg_optics_result;'
    t_sql7 = 'select avg(score) from unknown_data.random_result;'
    t_sql8 = 'select avg(score) from unknown_data.avg_result;'
    test_sql = [t_sql1, t_sql2, t_sql3, t_sql4, t_sql5, t_sql6, t_sql7, t_sql8]
    # 清表sql
    sql1 = 'TRUNCATE TABLE unknown_data.optics_result;'
    sql2 = 'TRUNCATE TABLE unknown_data.dbscan_result;'
    sql3 = 'TRUNCATE TABLE unknown_data.random_result;'
    sql4 = 'TRUNCATE TABLE unknown_data.avg_result;'
    sql5 = 'TRUNCATE TABLE unknown_data.avg_optics_result;'
    sql6 = 'TRUNCATE TABLE unknown_data.avg_dbscan_result;'
    sql7 = 'TRUNCATE TABLE unknown_data.k_means_result;'
    sql8 = 'TRUNCATE TABLE unknown_data.avg_k_means_result;'
    clear_sql = [sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8]
    # incline data
    sql = 'select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;'
    # air data
    # sql = 'select value, count(value) from unknown_data.air WHERE parameter = \'pm1\' and country = \'US\' group by value;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    print(same_data)
    # 聚类分层
    optics_layer = st.OPTICS(same_data, Eps=Eps, MinPts=MinPts)
    dbscan_layer = st.DBSCAN(same_data, Eps=Eps, MinPts=MinPts)
    k_means_layer = st.k_means_three(list(same_data.keys()))
    print('DBSCAN:')
    for i in dbscan_layer:
        print(i)
    print('OPTICS:')
    for i in optics_layer:
        print(i)
    print('K-MEANS:')
    for i in k_means_layer:
        print(i)
    # 查询数据
    # incline data
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    # air data
    # sql = 'select value from unknown_data.air WHERE parameter = \'pm1\' and country = \'US\' group by value;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    data = DataFrame(sql_result, columns=['score']).astype('float')
    # 数据分层
    opdata, zero_data = st.spilt_data_by_layer(optics_layer, data)
    dbdata, zero_data = st.spilt_data_by_layer(dbscan_layer, data)
    kmdata, zero_data = st.spilt_data_by_layer(k_means_layer, data)
    data_sum = len(sql_result)
    # 测试数据
    stand = 0.000389
    test_result = []
    t_result = []
    ind = []
    # incline data
    for sample_sum in range(500, 15001, 500):
    # air data
    # for sample_sum in range(100, 400, 100):
        ind.append(sample_sum)
        print(sample_sum)
        t_result.clear()
        # 清空数据表
        for sql in clear_sql:
            cursor.execute(sql)
        # K-MEANS
        st.sampling_data(engine, kmdata, data_sum, sample_sum, zero_data, 'k_means_result')
        st.avg_sampling_data(engine, kmdata, data_sum, sample_sum, zero_data, 'avg_k_means_result')
        # DBSCAN
        st.sampling_data(engine, dbdata, data_sum, sample_sum, zero_data, 'dbscan_result')
        st.avg_sampling_data(engine, dbdata, data_sum, sample_sum, zero_data, 'avg_dbscan_result')
        # OPTICS
        st.sampling_data(engine, opdata, data_sum, sample_sum, zero_data, 'optics_result')
        st.avg_sampling_data(engine, opdata, data_sum, sample_sum, zero_data, 'avg_optics_result')
        # RANDOM
        df_sample = data.sample(frac=sample_sum / data_sum, replace=False, axis=0)
        df_sample.to_sql('random_result', con=engine, if_exists='append', index=False, chunksize=100000)
        # AVG
        avg_sampling(data, sample_sum, engine)
        # TEST: K-MEANS, DBSCAN, OPTICS, RANDOM, AVG
        for test in test_sql:
            cursor.execute(test)
            result = cursor.fetchall()
            avg = round(float(result[0][0]), 6)
            accuracy = round(abs(avg - stand) / stand * 100, 6)
            t_result.append(accuracy)
        t = t_result.copy()
        test_result.append(t)
    # 存储结果
    columns_list = ['K-MEANS', 'avg_K-MEANS', 'DBSCAN', 'avg_DBSCAN', 'OPTICS', 'avg_OPTICS', 'RANDOM', 'AVG']
    df = DataFrame(test_result, columns=columns_list)
    df.index = ind
    print(df)
    path = 'temp_Clustering_accuracy' + str(write_count) + '.csv'
    df.to_csv(path)


if __name__ == '__main__':
    # incline data
    Eps = 0.0022
    MinPts = 8
    # air data
    #Eps = 0.2
    # MinPts = 60
    write_count = 10
    for i in range(1):
        real_data_test()
        write_count += 1