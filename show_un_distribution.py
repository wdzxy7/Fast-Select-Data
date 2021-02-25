import matplotlib.pyplot as plt
import pymysql
import numpy as np


def show():
    plt.figure(figsize=(40, 8))
    arr = np.array(t_data)
    plt.hist(arr, bins=100)
    '''
    x = [j for j in range(0, 1000, 10)]
    plt.xticks(x)
    '''
    plt.title('15')
    plt.show()
    print(max(arr), min(arr))


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    data = []
    t_data = []
    # for num in range(1, 36):
    # print(num)-

    t_data.clear()
    # sql = 'select score from unknown_data.data' + str(num) + ';'
    # sql = 'SELECT score FROM unknown_data.data3 WHERE `index`=21021002103010031321 ORDER BY score ASC;'
    sql = 'SELECT `value` FROM unknown_data.air WHERE locationId=63094;'
    cursor.execute(sql)
    res = cursor.fetchall()
    print(res)
    for i in res:
        if float(i[0]) < 1000:
            t_data.append(i[0])
    show()
