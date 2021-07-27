'''
利用seaborn绘制数据分布图
查询air中某个id的数据然后绘制分布图
与matplotlib不同的是
seaborn画的分布图为光滑曲线
'''
import pymysql
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties


font = FontProperties(fname=r"c:\windows\fonts\SimHei.ttf", size=12)


def show():
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    sns.set_style(style='white')
    arr = np.array(t_data)
    # sns.distplot(arr, kde=True, hist=False, label=str(number), ax=)
    sns.distplot(arr, kde=True, hist=False)
    plt.legend()


if __name__ == '__main__':
    f, ax = plt.subplots(1, 1)
    numbers = ['10001001100130001121', '10001002100130001321', '10011001100110031221', '10011001100110011221',
               '10011002100110031221', '10011002100110031321', '10021001100110021421', '10021001100110021121',
               '10031001100110031221', '10031001100110011421', '10031002100140001221', '10031002100110031221']
    for number in numbers:
        connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
        cursor = connect.cursor()
        data = []
        t_data = []
        # sql = 'select score from unknown_data.data' + str(num) + ';'
        sql = 'SELECT score FROM unknown_data.data3 WHERE `index`=' + number + ';'
        # sql = 'SELECT `value` FROM unknown_data.air WHERE locationId=' + number + ';'
        # sql = 'select value from unknown_data.air where country=\'IE\' and parameter=\'pm10\' group by value;'
        cursor.execute(sql)
        res = cursor.fetchall()
        print(res)
        for i in res:
            if -10 < float(i[0]) < 1000:
                t_data.append(i[0])
        print(len(t_data))
        show()
        plt.ylabel(u'密度', fontproperties=font)
        plt.xlabel(u'取值', fontproperties=font)
        plt.show()
