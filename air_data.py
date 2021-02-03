import single_test as st
import pymysql


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select value, count(value) from unknown_data.single_data where parameter=\'pm10\' group by value;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    layer2 = st.DBSCAN(same_data, Eps=0.2, MinPts=10)