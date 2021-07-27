'''
test1用于测试eps和minpts对于聚类结果的影响
test2用于测试k-means聚类运算时间花费
'''
import cluster_sample_algorithm as st
from pandas import DataFrame
from sklearn.cluster import KMeans
import numpy as np
import sql_connect
import time


# 测试eps 和 minpts取值
def test1():
    eps = 5.01
    minpts = 2
    sql = 'select value, count(value) from unknown_data.air where locationId=7990 group by value;'
    sql_con = sql_connect.Sql_c()
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    dbscan_layer = st.DBSCAN(same_data, Eps=eps, MinPts=minpts)
    for lay in dbscan_layer:
        print(lay)


def test2():
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    sql_con = sql_connect.Sql_c()
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    data = DataFrame(res, columns=['value'], dtype=float)
    arr = data['value']
    values = []
    for i in arr:
        values.append((i, 0))
    arr = np.array(values)
    costs = []
    for i in range(100):
        t1 = time.perf_counter()
        cluster = KMeans(n_clusters=10).fit(arr)
        t2 = time.perf_counter()
        cost = t2 - t1
        costs.append(cost)
    df = DataFrame(costs)
    des = df.describe()
    des.to_csv('time_cost3.csv')

test1()