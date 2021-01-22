import pymysql
from pandas import DataFrame
from decimal import Decimal
import pandas as pd


if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    # 抽样
    columns_list = ['biological', 'math', 'music', 'political', 'chemistry', 'chinese', 'geography', 'history',
                    'Line_generation', 'art', 'english', 'physics']
    # 设置第一行的标准数据 
    first_data = [[400.2163, 400.0873, 400.3409, 400.0527, 400.0152, 400.4353, 400.0475, 400.0209, 400.1792, 399.8196,
                   399.9599, 399.1788]]
    df = DataFrame(first_data, columns=columns_list)
    print(df)
    for sample_num in range(1, 21):
        for term_num in range(1, 4):
            sql = 'Select major,avg(score) from grades.stratified_sample' + str(sample_num) + ' where term = ' + str(term_num) + ' group by major;'
            print(sql)
            cursor.execute(sql)
            result = cursor.fetchall()
            for re in result:
                write_column = re[0]
                write_data = re[1]
                df.loc[int(sample_num), write_column] = write_data
    print(df)
    ind = []
    sam = Decimal('0.005')
    c = Decimal('0.005')
    for i in range(20):
        ind.append(str(sam) + '%')
        sam = sam + c
    ind.insert(0, '100%')
    df.index = ind
    print(df)
    df.to_csv('accuracy.csv')
