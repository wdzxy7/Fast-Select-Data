import single_test as st
import pymysql


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'SELECT value FROM unknown_data.single_data WHERE parameter = \'pm1\';'
    cursor.execute(sql)
    res = cursor.fetchall()
    data = []
    for i in res:
        data.append(float(i[0]))
    layer = st.DBSCAN(data, Eps=0.2, MinPts=5)
    print(layer)