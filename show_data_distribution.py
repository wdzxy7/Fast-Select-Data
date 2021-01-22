import hashlib
import pylab as pl
import pymysql
import numpy as np

if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select score from grades.exponential_data order by score;'
    cursor.execute(sql)
    res = cursor.fetchall()
    data = []
    for i in res:
        data.append(i[0])
    data = np.array(data)
    print(data)
    print('-------------------------------------------------------------')
    print(np.sort(data))
    pl.hist(data, 100)
    pl.title('exponential_data')
    pl.show()
