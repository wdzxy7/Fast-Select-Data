import hashlib
import pylab as pl
import pymysql
import numpy as np


# 查询数据绘制数据分布直方图，确定数据大致分布类型，倾斜情况
if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    pa = ['pm1', 'pm10', 'pm25', 'um010', 'um025', 'um100']
    country = ['IE', 'US']
    for c in country:
        for p in pa:
            ti = c + p
            sql = 'SELECT value FROM unknown_data.air WHERE parameter = \'' + p + '\' and country = \'' + c + '\';'
            print(sql)
            cursor.execute(sql)
            res = cursor.fetchall()
            data = []
            for i in res:
                data.append(i[0])
            data = np.array(data)
            pl.hist(data, 100)
            pl.title(ti)
            pl.show()
