'''
这个代码用于测试不同k的取值对k-means聚类抽样结果的影响
'''
import numpy as np
import sql_connect
from pandas import DataFrame
import cluster_sample_algorithm as st
from sklearn.cluster import KMeans


def get_cluster(cluster, data):
    result = []
    for i, j in zip(cluster.labels_, data):
        tup = (i, j)
        result.append(tup)
    result = set(result)
    result_dict = {}
    for i in result:
        try:
            result_dict[i[0]].append(float(i[1][0]))
        except:
            result_dict[i[0]] = []
            result_dict[i[0]].append(float(i[1][0]))
    result = []
    for key in result_dict:
        result.append(sorted(result_dict[key]))
    return result


if __name__ == '__main__':
    t_sql2 = 'select avg(score) from unknown_data.proportion_k_means_result;'
    test_sql = [t_sql2]
    # 清表sql
    sql8 = 'TRUNCATE TABLE unknown_data.proportion_k_means_result;'
    clear_sql = [sql8]
    sql_con = sql_connect.Sql_c()
    sql = 'select value from unknown_data.air where locationId=7983;'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    data = DataFrame(res, columns=['score']).astype('float')
    data_sum = len(res)
    values = []
    for i in res:
        values.append((i[0], 0))
    arr = np.array(values)
    test_result = []
    t_result = []
    for i in range(1, 11):
        for k in range(2, 11):
            cluster = KMeans(n_clusters=k).fit(arr)
            k_means_layer = get_cluster(cluster, values)
            kmdata = st.spilt_data_by_layer(k_means_layer, data)
            for rate in range(5, 51, 5):
                sample_sum = data_sum * rate / 100
                t_result.clear()
                # 清空数据表
                for sql in clear_sql:
                    sql_con.cursor.execute(sql)
                st.proportion_sample_data(sql_con.engine, kmdata, data_sum, sample_sum, 'proportion_k_means_result')
                for test in test_sql:
                    sql_con.cursor.execute(test)
                    result = sql_con.cursor.fetchall()
                    avg = round(float(result[0][0]), 6)
                    accuracy = round(abs(avg - 7.1809558) / 7.1809558 * 100, 6)
                    t_result.append(accuracy)
                t = t_result.copy()
                test_result.append(t)
            df = DataFrame(test_result, columns=['proportion_K-MEANS'])
            print(i, k)
            path = 'result/k-means/k' + str(k) + '.10_Clustering_accuracy' + str(i) + '.csv'
            df.to_csv(path, index=False)
            test_result.clear()
