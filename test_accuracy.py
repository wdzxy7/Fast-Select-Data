import pymysql
from pandas import DataFrame
from decimal import Decimal
import cluster_sample_algorithm as csa
import sql_connect


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
    hist = round(data_sum / sample_sum)
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


def real_data_test(data_type):
    Eps = parameter[data_type][0]
    MinPts = parameter[data_type][1]
    sql_con = sql_connect.Sql_c()
    # 测试sql
    t_sql1 = 'select avg(score) from unknown_data.k_means_result;'
    t_sql2 = 'select avg(score) from unknown_data.proportion_k_means_result;'
    t_sql3 = 'select avg(score) from unknown_data.dbscan_result;'
    t_sql4 = 'select avg(score) from unknown_data.optics_result;'
    t_sql5 = 'select avg(score) from unknown_data.random_result;'
    test_sql = [t_sql1, t_sql2, t_sql3, t_sql4, t_sql5]
    # 清表sql
    sql1 = 'TRUNCATE TABLE unknown_data.optics_result;'
    sql2 = 'TRUNCATE TABLE unknown_data.dbscan_result;'
    sql3 = 'TRUNCATE TABLE unknown_data.random_result;'
    sql4 = 'TRUNCATE TABLE unknown_data.k_means_result;'
    sql5 = 'TRUNCATE TABLE unknown_data.proportion_k_means_result;'
    clear_sql = [sql1, sql2, sql3, sql4, sql5]
    sql = select_count_sql[data_type]
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    # 聚类分层
    optics_layer = csa.OPTICS(same_data, Eps=Eps, MinPts=MinPts)
    dbscan_layer = csa.DBSCAN(same_data, Eps=Eps, MinPts=MinPts)
    k_means_layer = csa.k_means_three(list(same_data.keys()))
    print('DBSCAN:')
    for i in dbscan_layer:
        print(sorted(i))
    print('OPTICS:')
    for i in optics_layer:
        print(sorted(i))
    print('K-MEANS:')
    for i in k_means_layer:
        print(sorted(i))
    # 查询数据
    sql = select_data_sql[data_type]
    sql_con.cursor.execute(sql)
    sql_result = sql_con.cursor.fetchall()
    data = DataFrame(sql_result, columns=['score']).astype('float')
    # 数据分层
    opdata = csa.spilt_data_by_layer(optics_layer, data)
    dbdata = csa.spilt_data_by_layer(dbscan_layer, data)
    kmdata = csa.spilt_data_by_layer(k_means_layer, data)
    data_sum = len(sql_result)
    # 测试数据
    stand = stand_avg[data_type]
    test_result = []
    t_result = []
    ind = []
    start = run_range[0]
    end = run_range[1]
    pace = run_range[2]
    for rate in range(start, end, pace):
        sample_sum = int(data_sum * (rate / 100))
        ind.append(sample_sum)
        print(sample_sum)
        t_result.clear()
        # 清空数据表
        for sql in clear_sql:
            sql_con.cursor.execute(sql)
        # K-MEANS
        csa.sampling_data(sql_con.engine, kmdata, data_sum, sample_sum, 'k_means_result')
        # csa.avg_sampling_data(sql_con.engine, kmdata, data_sum, sample_sum, 'avg_k_means_result')
        csa.proportion_sample_data(sql_con.engine, kmdata, data_sum, sample_sum, 'proportion_k_means_result')
        # DBSCAN
        csa.sampling_data(sql_con.engine, dbdata, data_sum, sample_sum, 'dbscan_result')
        # csa.avg_sampling_data(sql_con.engine, dbdata, data_sum, sample_sum, 'avg_dbscan_result')
        # csa.proportion_sample_data(sql_con.engine, dbdata, data_sum, sample_sum, 'proportion_dbscan_result')
        # OPTICS
        csa.sampling_data(sql_con.engine, opdata, data_sum, sample_sum, 'optics_result')
        # csa.avg_sampling_data(sql_con.engine, opdata, data_sum, sample_sum, 'avg_optics_result')
        # csa.proportion_sample_data(sql_con.engine, opdata, data_sum, sample_sum, 'proportion_optics_result')
        # RANDOM
        df_sample = data.sample(frac=sample_sum / data_sum, replace=False, axis=0)
        df_sample.to_sql('random_result', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)
        # TEST: K-MEANS, DBSCAN, OPTICS, RANDOM
        for test in test_sql:
            sql_con.cursor.execute(test)
            result = sql_con.cursor.fetchall()
            avg = round(float(result[0][0]), 6)
            accuracy = round(abs(avg - stand) / stand * 100, 6)
            t_result.append(accuracy)
        t = t_result.copy()
        test_result.append(t)
    # 存储结果
    columns_list = ['K-MEANS', 'proportion_K-MEANS', 'DBSCAN', 'OPTICS', 'RANDOM']
    df = DataFrame(test_result, columns=columns_list)
    # df.index = ind
    print(df)
    path = data_type + '_Clustering_accuracy' + str(write_count) + '.csv'
    df.to_csv(path, index=False)


if __name__ == '__main__':
    # 查询数据sql
    select_data_sql = {
        'air': 'select value from unknown_data.air WHERE parameter = \'pm1\' and country = \'US\'',
        'incline': 'select score from unknown_data.data3 where `index`=22021001101410011321;',
        'air_incline': 'select value from unknown_data.air where locationId=63094;',
        'incline2': 'select score from unknown_data.data3 where `index`=21021003104010031120;'
    }
    # 查询数据分布sql
    select_count_sql = {
        'air': 'select value, count(value) from unknown_data.air WHERE parameter = \'pm1\' and country = \'US\' group by value;',
        'incline': 'select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;',
        'air_incline': 'select value, count(value) from unknown_data.air where locationId=63094 group by value;',
        'incline2': 'select score, count(score) from unknown_data.data3 where `index`=21021003104010031120 group by score;'
    }
    # 循环设置
    run_range = [5, 51, 5]
    # 设置Eps，MinPts
    parameter = {
        'air': [1, 15],
        'incline': [0.0022, 7],
        'air_incline': [0.111, 10],
        'incline2': [1.001, 6]
    }
    # 标准平均值
    stand_avg = {
        'incline': 0.000389,
        'air': 3.97103,
        'air_incline': 0.0177754,
        'incline2': 0.137269
    }
    # 写入文件编号
    write_count = 1
    # 测试次数
    run_times = 10
    for k in range(run_times):
        real_data_test('air_incline')
        write_count += 1