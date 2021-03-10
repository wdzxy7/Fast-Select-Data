import re
import numpy as np
import sql_connect  # 自写库sql连接
import cluster_sample_algorithm as csa  # 自写库
from pandas import DataFrame
from openpyxl import Workbook
import cluster_sampling as cs  # 自写库
from sklearn.cluster import KMeans


# 直接随机抽样
def all_random(data):
    sql_con.cursor.execute('TRUNCATE TABLE unknown_data.all_random;')
    df_sample = data.sample(frac=sample_sum / data_sum, replace=False, axis=0)
    df_sample.to_sql('all_random', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)


# 先分组然后随机抽样
def group_random(data):
    sql_con.cursor.execute('TRUNCATE TABLE unknown_data.group_random;')
    locations = data['locationId']
    locations = list(set(locations))
    store_df = DataFrame([], columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                                      'unit', 'latitude', 'longitude'])
    # 根据locationid分组然后随机抽样，每组抽样数量按比例分配
    for locationid in locations:
        small_data = data.loc[data['locationId'] == locationid]
        data_length = len(small_data)
        sam_sum = sample_sum * data_length / data_sum
        sam = sam_sum / data_length
        sample = small_data.sample(frac=sam, replace=False, axis=0)
        store_df = store_df.append(sample, ignore_index=True)
    store_df.to_sql('group_random', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)


# 数据进行聚类然后返回根据聚类分层后的数据 给all_cluster使用
def cluster(data):
    sql = 'select value, count(value) from unknown_data.air where locationId=62256 or locationId=62880 or ' \
          'locationId=63094 or locationId=66355 or locationId=64704 or locationId=62693 group by value;'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    arr = data['value']
    values = []
    for i in arr:
        values.append((i, 0))
    arr = np.array(values)
    cluster = KMeans(n_clusters=10).fit(arr)
    k_means_layer = get_cluster(cluster, values)
    dbscan_layer = csa.DBSCAN(same_data, Eps=0.4, MinPts=6)
    optics_layer = csa.OPTICS(same_data, Eps=0.4, MinPts=6)
    opdata = cs.spilt_data_by_layer(optics_layer, data)
    dbdata = cs.spilt_data_by_layer(dbscan_layer, data)
    kmdata = cs.spilt_data_by_layer(k_means_layer, data)
    return dbdata, opdata, kmdata


# 所有数据总体聚类根据聚类结果随机抽样
def all_cluster(dbdata, opdata, kmdata):
    sql1 = 'TRUNCATE TABLE unknown_data.all_dbscan_random;'
    sql2 = 'TRUNCATE TABLE unknown_data.all_optics_random;'
    sql3 = 'TRUNCATE TABLE unknown_data.all_k_means_random;'
    sql4 = 'TRUNCATE TABLE unknown_data.all_proportion_k_means_random;'
    clear_sql = [sql1, sql2, sql3, sql4]
    for clear in clear_sql:
        sql_con.cursor.execute(clear)
    # 抽样
    # K-MEANS
    cs.sampling_all_data(sql_con.engine, kmdata, data_sum, sample_sum, 'all_k_means_random')
    cs.proportion_sample_data(sql_con.engine, kmdata, data_sum, sample_sum, 'all_proportion_avg_k_means_random')
    # DBSCAN
    cs.sampling_all_data(sql_con.engine, dbdata, data_sum, sample_sum, 'all_dbscan_random')
    # OPTICS
    cs.sampling_all_data(sql_con.engine, opdata, data_sum, sample_sum, 'all_optics_random')


# 把数据先分组然后进行组内的聚类运算抽样，每组抽样数量按比例分配
def group_cluster(data):
    sql1 = 'TRUNCATE TABLE unknown_data.group_dbscan_random;'
    sql2 = 'TRUNCATE TABLE unknown_data.group_optics_random;'
    sql3 = 'TRUNCATE TABLE unknown_data.group_k_means_random;'
    sql4 = 'TRUNCATE TABLE unknown_data.group_proportion_k_means_random;'
    clear_sql = [sql1, sql2, sql3, sql4]
    for clear in clear_sql:
        sql_con.cursor.execute(clear)
    parameter = {
        '62256': [0.2, 9],
        '63094': [0.1001, 10],
    }
    sqls = {
        '62256': 'select avg(value), count(value) from unknown_data.air where locationId=62256 group by value;',
        '63094': 'select avg(value), count(value) from unknown_data.air where locationId=63094 group by value;'
    }
    for key in parameter.keys():
        eps = parameter[key][0]
        minpts = parameter[key][1]
        sql = sqls[key]
        sql_con.cursor.execute(sql)
        res = sql_con.cursor.fetchall()
        same_data = {}
        for i in res:
            same_data[float(i[0])] = int(i[1])
        # 给k-means使用
        location_data = data.loc[data['locationId'] == key]
        arr = location_data['value']
        data_length = len(arr)
        sample = sample_sum * (data_length / data_sum)
        values = []
        for i in arr:
            values.append((i, 0))
        arr = np.array(values)
        cluster = KMeans(n_clusters=10).fit(arr)
        # 数据聚类运算
        k_means_layer = get_cluster(cluster, values)
        dbscan_layer = csa.DBSCAN(same_data, Eps=eps, MinPts=minpts)
        optics_layer = csa.OPTICS(same_data, Eps=eps, MinPts=minpts)
        # 根据聚类结果进行数据分层
        opdata = cs.spilt_data_by_layer(optics_layer, location_data)
        dbdata = cs.spilt_data_by_layer(dbscan_layer, location_data)
        kmdata = cs.spilt_data_by_layer(k_means_layer, location_data)
        # 利用上述聚类分层结果进行抽样
        # K-MEANS
        cs.sampling_all_data(sql_con.engine, kmdata, data_length, sample, 'group_k_means_random')
        cs.proportion_sample_data(sql_con.engine, kmdata, data_length, sample, 'group_proportion_k_means_random')
        # DBSCAN
        cs.sampling_all_data(sql_con.engine, dbdata, data_length, sample, 'group_dbscan_random')
        # OPTICS
        cs.sampling_all_data(sql_con.engine, opdata, data_length, sample, 'group_optics_random')


# 把调用官方k-means算法的结果转换出来
def get_cluster(cluster, data):
    result = []
    for i, j in zip(cluster.labels_, data):
        tup = (i, j)
        result.append(tup)
    result = set(result)
    result_dict = {}
    for i in result:
        try:
            result_dict[i[0]].append(i[1][0])
        except:
            result_dict[i[0]] = []
            result_dict[i[0]].append(i[1][0])
    result = []
    for key in result_dict:
        result.append(sorted(result_dict[key]))
    return result


# 计算误差函数
def get_error_rate():
    t_sql1 = 'select avg(value), locationId from unknown_data.all_dbscan_random group by locationId;'
    t_sql2 = 'select avg(value), locationId from unknown_data.all_optics_random group by locationId;'
    t_sql3 = 'select avg(value), locationId from unknown_data.all_k_means_random group by locationId;'
    t_sql4 = 'select avg(value), locationId from unknown_data.all_proportion_k_means_random group by locationId;'
    t_sql5 = 'select avg(value), locationId from unknown_data.group_dbscan_random group by locationId;'
    t_sql6 = 'select avg(value), locationId from unknown_data.group_optics_random group by locationId;'
    t_sql7 = 'select avg(value), locationId from unknown_data.group_k_means_random group by locationId;'
    t_sql8 = 'select avg(value), locationId from unknown_data.group_proportion_k_means_random group by locationId;'
    t_sql9 = 'select avg(value), locationId from unknown_data.all_random group by locationId;'
    t_sql10 = 'select avg(value), locationId from unknown_data.group_random group by locationId;'
    test_sql = [t_sql1, t_sql2, t_sql3, t_sql4, t_sql5, t_sql6, t_sql7, t_sql8, t_sql9, t_sql10]
    return_dict = {}
    res_dict = {}
    for sql in test_sql:
        sql_con.cursor.execute(sql)
        result = sql_con.cursor.fetchall()
        r = re.findall(r'from(.*?)group by', sql, re.IGNORECASE)
        name = r[0].replace(' ', '')
        name = name.split('.')
        name = name[1]
        res_dict.clear()
        # 计算误差
        for i in result:
            stand = stand_res[i[1]]
            error = abs(stand - float(i[0])) / stand * 100
            res_dict[i[1]] = error
        t = res_dict.copy()
        return_dict[name] = t
    return return_dict


# 结果写入excel
def write():
    wb = Workbook()
    excel = wb.active
    temp = res_dict['all_random']
    count = 2
    index = []
    for key in temp.keys():
        excel['A' + str(count)] = key
        index.append(key)
        count = count + 1
    place = 'B'
    for key in res_dict.keys():
        count = 1
        excel[place + str(count)] = key
        count = count + 1
        for i in index:
            try:
                excel[place + str(count)] = res_dict[key][i]
            except:
                excel[place + str(count)] = 0
            count = count + 1
        next_place = ord(place) + 1
        place = chr(next_place)
    save_name = 'air_result' + str(write_count) + '.xlsx'
    wb.save(save_name)


def main():
    # incline 62256 63094
    sql = 'select * from unknown_data.air where locationId=62256 or locationId=63094 or ' \
          'locationId=64704 or locationId=62693;'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    data = DataFrame(result, columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                                      'unit', 'latitude', 'longitude', 'id'])
    data = data.drop(['id'], axis=1)
    # 设置value类数据类型不然会报错
    data['value'] = data['value'].astype('float')
    # 进行各种抽样查询
    print('all_random')
    all_random(data)
    print('group_random')
    group_random(data)
    print('all_cluster')
    dbdata, opdata, kmdata = cluster(data)
    all_cluster(dbdata, opdata, kmdata)
    print('group_cluster')
    group_cluster(data)


if __name__ == '__main__':
    sample_sum = 8000 # 4461
    data_sum = 251722
    sql_con = sql_connect.Sql_c()
    sql = 'select locationId, avg(value) from unknown_data.air where locationId=62256 or locationId=63094  or locationId=62693 or locationId=64704 group by locationId;'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    stand_res = {}
    # 计算标准结果
    for i in res:
        stand_res[i[0]] = float(i[1])
    write_count = 1
    for i in range(1):
        main()
        res_dict = get_error_rate()
        write()
        write_count += 1