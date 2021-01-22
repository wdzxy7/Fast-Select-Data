import pymysql
import single_test as st
from openpyxl import Workbook


def test():
    st.split_by_rate()
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'SELECT avg(score) FROM unknown_data.small_test;'
    cursor.execute(sql)
    result = cursor.fetchall()
    avg = float(result[0][0])
    error_rate = abs(avg - 6.279684783367062) / 6.279684783367062
    print(avg, round(error_rate, 5))
    return avg, round(error_rate, 5)


if __name__ == '__main__':
    wb = Workbook()
    excel = wb.active
    excel['A1'] = 'avg'
    excel['B1'] = 'error_rate'
    s = 0
    for i in range(2, 102):
        a, error = test()
        excel['A' + str(i)] = a
        excel['B' + str(i)] = error
        s = s + error
    print(s / 100)
    wb.save('error_rate2.xlsx')