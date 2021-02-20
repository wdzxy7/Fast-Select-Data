import numpy as np
import pymysql
from sklearn.cluster import DBSCAN


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321 order by score;'
    cursor.execute(sql)
    result = cursor.fetchall()
    data = []
    for i in result:
        data.append(float(i[0]))
    arr = np.array(data)
    print(arr)
    cluster = DBSCAN(eps=0.002, min_samples=8).fit(arr)
    print(cluster.labels_)