import math
import sql_connect
from pandas import DataFrame


if __name__ == '__main__':
    sql_conn = sql_connect.Sql_c()
    sql_conn.cursor.execute('select score from unknown_data.data3 where `index`=22021001101410011321;')
    result = sql_conn.cursor.fetchall()
    df = DataFrame(result).astype('float')
    std = df.std()
    std = std * std
    s = 4 / (3 * len(df))
    eps = math.pow(s, 1/5) * float(std)
    print(eps)
    sql_conn.cursor.execute('select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;')
    res = sql_conn.cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    data = same_data.keys()
    point_dict = {}
    for core in data:  # 遍历所有找出核心点
        point = 0
        for score in data:  # 找出核心点在Eps邻域中的点
            same_point = same_data[score]  # 相同点数量
            s = core - score
            distance = s / eps
            if distance <= 0.5:
                point = point + same_point
        point_dict[core] = point
    for key in point_dict.keys():
        print(key, point_dict[key])
    '''
    for i in df:
        k = i - eps
        if k > 0.5:
            k = 0
        else:
            k = 1
    '''