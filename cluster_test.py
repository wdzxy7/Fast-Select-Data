import cluster_sample_algorithm as st
from pandas import DataFrame
from sklearn.cluster import KMeans
import numpy as np
import sql_connect
import time


# 测试eps 和 minpts取值
def test1():
    eps = 0.403
    minpts = 200
    sql = 'select value, count(value) from unknown_data.air where locationId=62256 or locationId=62880 or ' \
          'locationId=63094 or locationId=66355 or locationId=64704 or locationId=62693 group by value;'
    sql_con = sql_connect.Sql_c()
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    costs = []
    for i in range(10):
        print(i)
        t1 = time.perf_counter()
        dbscan_layer = st.OPTICS(same_data, Eps=eps, MinPts=minpts)
        t2 = time.perf_counter()
        cost = t2 - t1
        costs.append(cost)
    df = DataFrame(costs)
    des = df.describe()
    des.to_csv('time_cost2.csv')
    '''
    for i in dbscan_layer:
        print(sorted(i))
    '''


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