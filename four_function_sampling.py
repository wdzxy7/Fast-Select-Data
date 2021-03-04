import single_test as st
import numpy as np
import sql_connect
import cluster_sampling as cs
from pandas import DataFrame
from sklearn.cluster import KMeans


def all_random(data):
    sql_con.cursor.execute('TRUNCATE TABLE unknown_data.all_random;')
    df_sample = data.sample(frac=sample_sum / data_sum, replace=False, axis=0)
    df_sample.to_sql('all_random', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)


def all_avg_random(data):
    sql_con.cursor.execute('TRUNCATE TABLE unknown_data.all_avg_random;')
    df_length = len(data)
    hist = round(data_sum / sample_sum)
    sam = 1 / hist
    store_df = DataFrame([], columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                                      'unit', 'latitude', 'longitude'])
    front = 0
    back = hist
    max_range = front + df_length - 1
    while front < max_range:
        small = data.loc[front:back, :]
        sample = small.sample(frac=sam, replace=False, axis=0)
        store_df = store_df.append(sample, ignore_index=True)
        front = back + 1
        back = back + hist
        if back > max_range:
            back = max_range
    store_df.to_sql('all_avg_random', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)


def group_random(data):
    sql_con.cursor.execute('TRUNCATE TABLE unknown_data.group_random;')
    locations = data['locationId']
    locations = list(set(locations))
    print(locations)
    store_df = DataFrame([], columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                                      'unit', 'latitude', 'longitude'])
    for locationid in locations:
        small_data = data.loc[data['locationId'] == locationid]
        data_length = len(small_data)
        sam_sum = sample_sum * data_length / data_sum
        sam = sam_sum / data_length
        sample = small_data.sample(frac=sam, replace=False, axis=0)
        store_df = store_df.append(sample, ignore_index=True)
    store_df.to_sql('group_random', con=sql_con.engine, if_exists='append', index=False, chunksize=100000)


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
    dbscan_layer = st.DBSCAN(same_data, Eps=0.4, MinPts=6)
    optics_layer = st.OPTICS(same_data, Eps=0.4, MinPts=6)
    opdata = cs.spilt_data_by_layer(optics_layer, data)
    dbdata = cs.spilt_data_by_layer(dbscan_layer, data)
    kmdata = cs.spilt_data_by_layer(k_means_layer, data)
    return dbdata, opdata, kmdata


def all_cluster(dbdata, opdata, kmdata):
    sql1 = 'TRUNCATE TABLE unknown_data.all_dbscan_random;'
    sql2 = 'TRUNCATE TABLE unknown_data.all_avg_dbscan_random;'
    sql3 = 'TRUNCATE TABLE unknown_data.all_optics_random;'
    sql4 = 'TRUNCATE TABLE unknown_data.all_avg_optics_random;'
    sql5 = 'TRUNCATE TABLE unknown_data.all_k_means_random;'
    sql6 = 'TRUNCATE TABLE unknown_data.all_avg_k_means_random;'
    clear_sql = [sql1, sql2, sql3, sql4, sql5, sql6]
    for clear in clear_sql:
        sql_con.cursor.execute(clear)
    # K-MEANS
    cs.sampling_all_data(sql_con.engine, kmdata, data_sum, sample_sum, 'all_k_means_random')
    cs.avg_sampling_all_data(sql_con.engine, kmdata, data_sum, sample_sum, 'all_avg_k_means_random')
    # DBSCAN
    cs.sampling_all_data(sql_con.engine, dbdata, data_sum, sample_sum, 'all_dbscan_random')
    cs.avg_sampling_all_data(sql_con.engine, dbdata, data_sum, sample_sum, 'all_avg_dbscan_random')
    # OPTICS
    cs.sampling_all_data(sql_con.engine, opdata, data_sum, sample_sum, 'all_optics_random')
    cs.avg_sampling_all_data(sql_con.engine, opdata, data_sum, sample_sum, 'all_avg_optics_random')
    pass


def group_cluster(data):
    sql1 = 'TRUNCATE TABLE unknown_data.group_dbscan_random;'
    sql2 = 'TRUNCATE TABLE unknown_data.group_avg_dbscan_random;'
    sql3 = 'TRUNCATE TABLE unknown_data.group_optics_random;'
    sql4 = 'TRUNCATE TABLE unknown_data.group_avg_optics_random;'
    sql5 = 'TRUNCATE TABLE unknown_data.group_k_means_random;'
    sql6 = 'TRUNCATE TABLE unknown_data.group_avg_k_means_random;'
    clear_sql = [sql1, sql2, sql3, sql4, sql5, sql6]
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
        sql_con.cursor.execute(sql)
        res = sql_con.cursor.fetchall()
        same_data = {}
        for i in res:
            same_data[float(i[0])] = int(i[1])
        location_data = data.loc[data['locationId'] == key]
        arr = location_data['value']
        data_length = len(arr)
        sample = sample_sum * (data_length / data_sum)
        values = []
        for i in arr:
            values.append((i, 0))
        arr = np.array(values)
        cluster = KMeans(n_clusters=10).fit(arr)
        k_means_layer = get_cluster(cluster, values)
        dbscan_layer = st.DBSCAN(same_data, Eps=eps, MinPts=minpts)
        optics_layer = st.OPTICS(same_data, Eps=eps, MinPts=minpts)
        opdata = cs.spilt_data_by_layer(optics_layer, location_data)
        dbdata = cs.spilt_data_by_layer(dbscan_layer, location_data)
        kmdata = cs.spilt_data_by_layer(k_means_layer, location_data)
        # K-MEANS
        cs.sampling_all_data(sql_con.engine, kmdata, data_length, sample, 'group_k_means_random')
        cs.avg_sampling_all_data(sql_con.engine, kmdata, data_length, sample, 'group_avg_k_means_random')
        # DBSCAN
        cs.sampling_all_data(sql_con.engine, dbdata, data_length, sample, 'group_dbscan_random')
        cs.avg_sampling_all_data(sql_con.engine, dbdata, data_length, sample, 'group_avg_dbscan_random')
        # OPTICS
        cs.sampling_all_data(sql_con.engine, opdata, data_length, sample, 'group_optics_random')
        cs.avg_sampling_all_data(sql_con.engine, opdata, data_length, sample, 'group_avg_optics_random')


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


def main():
    # incline 62256 63094
    sql = 'select * from unknown_data.air where locationId=62256 or locationId=63094 or ' \
          'locationId=64704 or locationId=62693;'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    data = DataFrame(result, columns=['locationId', 'location', 'city', 'country', 'utc', 'local', 'parameter', 'value',
                                      'unit', 'latitude', 'longitude', 'id'])
    data = data.drop(['id'], axis=1)
    data['value'] = data['value'].astype('float')
    # all_random(data)
    # group_random(data)
    print('all_avg')
    all_avg_random(data)
    print('all_cluster')
    dbdata, opdata, kmdata = cluster(data)
    all_cluster(dbdata, opdata, kmdata)
    print('group_cluster')
    group_cluster(data)


if __name__ == '__main__':
    sample_sum = 4461
    data_sum = 251722
    sql_con = sql_connect.Sql_c()
    main()