from decimal import Decimal
import pymysql
from pandas import DataFrame
import time
from sqlalchemy import create_engine
import random


def spilt_by_score():
    sample = 30
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select * from unknown_data.data3 where `index`=21021003103310031121 order by score;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df = DataFrame(result, columns=['id', 'index', 'time', 'score'])
    df['score'] = df['score'].astype('float')
    max_score = max(df['score'])
    min_score = min(df['score'])
    print(max_score, min_score)
    front = int(float(min_score))
    back = int(float(front)) + 10
    store_df = DataFrame([], columns=['id', 'index', 'time', 'score'])
    while front < int(float(max_score)):
        print(front, back)
        t = df.loc[(front <= df['score']) & (df['score'] < back)]
        sam = 3 / len(t)
        if sam > 1:
            sam = 1
        df_sample = t.sample(frac=sam, replace=False, axis=0)
        store_df = store_df.append(df_sample, ignore_index=True)
        front += 10
        back += 10
    store_df.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


def split_by_rate():
    sample = 1000000
    data_sum = 14333
    all_data = 36428644
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.small_test;'
    cursor.execute(sql)
    sql = 'select * from unknown_data.data3 where `index`=21021003103310031121 order by score;'
    cursor.execute(sql)
    result = cursor.fetchall()
    df = DataFrame(result, columns=['id', 'index', 'time', 'score'])
    # length = len(df)
    sample_length = data_sum / all_data * sample
    hist = data_sum / sample_length
    front = 0
    back = int(hist)
    sam = 1 / hist
    store_df = DataFrame([], columns=['id', 'index', 'time', 'score'])
    while front < data_sum:
        data = df.loc[front:back, :]
        df_sample = data.sample(frac=sam, replace=False, axis=0)
        store_df = store_df.append(df_sample, ignore_index=True)
        front = back
        back = back + hist
    store_df.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


def start_num(k, length):
    nums = set()
    while len(nums) < k:
        nums.add(random.randint(0, length))
    return list(nums)


def k_means_three(data):
    result = [[], [], []]
    num = start_num(3, len(data) - 1)
    u_list = []  # 当前计算的u
    for i in num:
        u_list.append(data[i])
    pre_u = [0, 0, 0]  # 存上一次的u结果
    distance = []  # 这个数到每个u的距离
    precision = 5
    while round(u_list[0], precision) != round(pre_u[0], precision) or round(u_list[1], precision) != \
            round(pre_u[1], precision) or round(u_list[2], precision) != round(pre_u[2], precision):
        for res in result:
            res.clear()
        pre_u.clear()
        for i in u_list:  # 更新pre_u
            pre_u.append(i)
        for score in data:
            distance.clear()
            for i in u_list:  # 计算distance
                distance.append(abs(i - score))
            m_dis = min(distance)  # 找出最小的distance
            local = distance.index(m_dis)  # 确定该distance的编号
            result[local].append(score)  # 存入列表
        # 计算新的u
        u_list.clear()
        for res in result:
            avg = sum(res) / len(res)
            u_list.append(avg)
    return result


def k_means_two(data):
    result = [[], []]
    num = start_num(2, len(data) - 1)
    u_list = []  # 当前计算的u
    for i in num:
        u_list.append(data[i])
    pre_u = [0, 0]  # 存上一次的u结果
    distance = []  # 这个数到每个u的距离
    precision = 5
    while round(u_list[0], precision) != round(pre_u[0], precision) or round(u_list[1], precision) != \
            round(pre_u[1], precision):
        for res in result:
            res.clear()
        pre_u.clear()
        for i in u_list:  # 更新pre_u
            pre_u.append(i)
        for score in data:
            distance.clear()
            for i in u_list:  # 计算distance
                distance.append(abs(i - score))
            m_dis = min(distance)  # 找出最小的distance
            local = distance.index(m_dis)  # 确定该distance的编号
            result[local].append(score)  # 存入列表
        # 计算新的u
        u_list.clear()
        for res in result:
            avg = sum(res) / len(res)
            u_list.append(avg)
    return result


def layered_by_k_means():
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.small_test;'
    cursor.execute(sql)
    sql = 'select distinct score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    result = cursor.fetchall()
    data = []
    for i in result:
        data.append(float(i[0]))
    layer = k_means_three(data)
    print(layer)
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    data_sum = len(sql_result)
    df_list = []
    # 取出每层的数据
    temp_df = DataFrame()
    for lay in layer:
        for score in lay:
            specimen = df.loc[df['score'] == score, :]
            temp_df = temp_df.append(specimen, ignore_index=True)
        t_df = temp_df.copy(deep=True)
        df_list.append(t_df)
        temp_df.drop(temp_df.index, inplace=True)
        temp_df.columns = ['score']
    denominator = 0
    molecular_list = []
    for df in df_list:
        w = len(df) / data_sum
        desc = df.describe()
        std = float(desc.iloc[2])
        s = w * std
        if s < 0.0000000000000001:
            s = 0
        denominator = denominator + s
        molecular = 788 * w * std
        molecular_list.append(molecular)
    for molecular, df in zip(molecular_list, df_list):
        specimen = molecular / denominator
        sam = float(specimen) / len(df)
        print(sam)
        if sam > 1:
            sam = 1
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        print(df_sample)
        df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


# 分为0，非0两层
def two_layer():
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.small_test;'
    cursor.execute(sql)
    sql = 'select distinct score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    result = cursor.fetchall()
    fzdata = []
    for i in result:
        if float(i[0]) == 0:
            continue
        fzdata.append(float(i[0]))
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    data_sum = len(sql_result)
    fz_df = DataFrame()
    z_df = df.loc[df['score'] == 0, :]
    # 取出非0数据
    for score in fzdata:
        specimen = df.loc[df['score'] == score, :]
        fz_df = fz_df.append(specimen, ignore_index=True)
    fz_df.columns = ['score']
    denominator = 0
    molecular_list = []
    df_list = [z_df, fz_df]
    for df in df_list:
        w = len(df) / data_sum  # 数据占比
        desc = df.describe()
        std = float(desc.iloc[2])  # 标准差
        s = w * std
        if s < 0.0000000000000001:
            s = 0
        denominator = denominator + s
        molecular = 788 * w * std
        molecular_list.append(molecular)
    for molecular, df in zip(molecular_list, df_list):
        specimen = molecular / denominator
        sam = float(specimen) / len(df)
        print(sam)
        if sam > 1:
            sam = 1
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        print(df_sample)
        df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


# 从非0数据项直接抽取
def without_zero():
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.small_test;'
    cursor.execute(sql)
    sql = 'select distinct score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    result = cursor.fetchall()
    fzdata = []
    for i in result:
        if float(i[0]) == 0:
            continue
        fzdata.append(float(i[0]))
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    fz_df = DataFrame()
    # 取出非0数据
    for score in fzdata:
        specimen = df.loc[df['score'] == score, :]
        fz_df = fz_df.append(specimen, ignore_index=True)
    fz_df.columns = ['score']
    df_sample = fz_df.sample(frac=12 / len(fz_df), replace=False, axis=0)
    print(df_sample)
    print(df_sample.describe())
    df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


# 把非0进行聚类后分层
def without_z_layer():
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.small_test;'
    cursor.execute(sql)
    sql = 'select distinct score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    result = cursor.fetchall()
    fzdata = []
    for i in result:
        if float(i[0]) == 0:
            continue
        fzdata.append(float(i[0]))
    layer = k_means_two(fzdata)
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    fz_df = DataFrame()
    # 取出非0数据
    for score in fzdata:
        specimen = df.loc[df['score'] == score, :]
        fz_df = fz_df.append(specimen, ignore_index=True)
    fz_df.columns = ['score']
    data_sum = len(fz_df)
    df_list = []
    # 取出每层的数据
    temp_df = DataFrame()
    print('spilt--------------------------------------------')
    for lay in layer:
        for score in lay:
            specimen = fz_df.loc[fz_df['score'] == score, :]
            temp_df = temp_df.append(specimen, ignore_index=True)
        t_df = temp_df.copy(deep=True)
        print(len(t_df), lay)
        df_list.append(t_df)
        temp_df.drop(temp_df.index, inplace=True)
        temp_df.columns = ['score']
    denominator = 0
    molecular_list = []
    print('层总量，总量，w，std，molecular')
    for df in df_list:
        w = len(df) / data_sum
        desc = df.describe()
        std = float(desc.iloc[2])
        s = w * std
        if s < 0.0000000000000001:
            s = 0
        denominator = denominator + s
        molecular = 12 * w * std
        print(len(df), data_sum, w, std, molecular)
        molecular_list.append(molecular)
    print('molecular', molecular_list)
    print('denominator:', denominator)
    print('sampling--------------------------------------------------------------')
    for molecular, df in zip(molecular_list, df_list):
        specimen = molecular / denominator  # 抽样数据量
        sam = specimen / len(df)
        print('data_sum:', specimen, sam)
        if sam > 1:
            sam = 1
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        '''
        if len(df_sample) == 0:
            df_sample = df.sample(frac=1 / len(df), replace=False, axis=0)
        '''
        print(df_sample)
        print('next--------------------------------------------------------')
        df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


def DBSCAN(data, Eps=0.002, MinPts=3):
    class_list = []
    f_core = set()  # 存放不是核心点
    y_core = {}  # 存放是核心点
    for core in data:  # 遍历所有找出核心点
        t_core = set()
        for score in data:  # 找出核心点在Eps邻域中的点
            if abs(core - score) <= Eps:
                t_core.add(score)
        if len(t_core) >= MinPts:  # 是核心点
            y_core[core] = t_core
        else:
            f_core.add(core)
    ct_cores = y_core.keys()  # 核心点集
    ct_cores = set(ct_cores)
    # 聚类
    while len(ct_cores) != 0:
        print(len(ct_cores))
        t = list(ct_cores)
        core = t.pop()
        friends = y_core[core]  # set类型
        now_class = friends.copy()
        pre_class = set()
        # 聚类运算
        while pre_class != now_class:
            pre_class = now_class.copy()
            now_class.clear()
            point_list = list(pre_class)
            for point in point_list:
                try:  # 核心点
                    core_friend = y_core[point]  # 获取这个点的邻域点
                    now_class = now_class | set(core_friend)
                except:  # 非核心点
                    now_class.add(point)
        class_list.append(now_class)
        ct_cores = ct_cores - now_class
    return class_list


def Layer_by_DBSCAN():
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select distinct score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    result = cursor.fetchall()
    data = []
    for i in result:
        data.append(float(i[0]))
    print('DBSCAN START')
    layer = DBSCAN(data)
    for i in layer:
        print(sorted(i))


if __name__ == '__main__':
    Layer_by_DBSCAN()
