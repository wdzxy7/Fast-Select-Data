from decimal import Decimal
import pymysql
from pandas import DataFrame
import time
from sqlalchemy import create_engine
import random
from openpyxl import Workbook


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


def spilt_data_by_layer(layer, data_df):
    return_df = []
    temp_df = DataFrame()
    for lay in layer:
        for score in lay:
            specimen = data_df.loc[data_df['score'] == score, :]
            temp_df = temp_df.append(specimen, ignore_index=True)
        t_df = temp_df.copy(deep=True)
        return_df.append(t_df)
        temp_df.drop(temp_df.index, inplace=True)
        temp_df.columns = ['score']
    zero_data = data_df.loc[data_df['score'] == 0, :]
    return return_df, zero_data


def sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database):
    denominator = 0
    lest = sample_sum
    molecular_list = []
    # 计算公式所需的每一层的分子分母
    for df in df_list:
        w = len(df) / data_sum
        desc = df.describe()
        std = round(float(desc.iloc[2]), 10)
        s = w * std
        if s < 0.0000000000000001:
            s = 0
        denominator = denominator + s
        molecular = sample_sum * w * std   # 分母 = 抽样量 * 该层数据占比 * 该层标准差
        molecular_list.append(molecular)
    # 利用公式计算出每层抽取样本数量进行抽样
    for molecular, df in zip(molecular_list, df_list):
        specimen = molecular / denominator
        sam = float(specimen) / len(df)
        if sam > 1:
            # print(df)
            # print(molecular, denominator, specimen)
            sam = 1
        df_sample = df.sample(frac=sam, replace=False, axis=0)
        # print(sam, len(df_sample))
        # print('-----------------------------------')
        lest = lest - len(df_sample)
        df_sample.to_sql(database, con=engine, if_exists='append', index=False, chunksize=100000)
    # 抽取0项
    try:
        sam = lest / len(zero_data)
        # print(sam)
        if sam > 1:
            sam = 1
        elif sam < 0:
            sam = 0
        df_sample = zero_data.sample(frac=sam, replace=False, axis=0)
        df_sample.to_sql(database, con=engine, if_exists='append', index=False, chunksize=100000)
    except:
        pass


def avg_sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database):
    denominator = 0
    lest = sample_sum
    molecular_list = []
    # 计算公式所需的每一层的分子分母
    for df in df_list:
        w = len(df) / data_sum
        desc = df.describe()
        std = round(float(desc.iloc[2]), 10)
        s = w * std
        if s < 0.0000000000000001:
            s = 0
        denominator = denominator + s
        molecular = sample_sum * w * std  # 分母 = 抽样量 * 该层数据占比 * 该层标准差
        molecular_list.append(molecular)
    # 利用公式计算出每层抽取样本数量进行抽样
    for molecular, df in zip(molecular_list, df_list):
        specimen = molecular / denominator  # 抽样数量
        df_length = len(df)
        sam = float(specimen) / df_length
        # 需要全部抽取
        if sam > 1:
            sam = 1
            df_sample = df.sample(frac=sam, replace=False, axis=0)
        else:
            hist = int(df_length / specimen)
            sam = 1 / hist
            df_sample = DataFrame([], columns=['score']).astype('float')
            front = 0
            back = 0
            max_range = front + df_length - 1
            while front < max_range:
                data = df.loc[front:back, :]
                sample = data.sample(frac=sam, replace=False, axis=0)
                df_sample = df_sample.append(sample, ignore_index=True)
                front = back + 1
                back = back + hist
                if back > max_range:
                    back = max_range
        lest = lest - len(df_sample)
        df_sample.to_sql(database, con=engine, if_exists='append', index=False, chunksize=100000)
    # 抽取0项
    try:
        sam = lest / len(zero_data)
        # print(sam)
        if sam > 1:
            sam = 1
        elif sam < 0:
            sam = 0
        df_sample = zero_data.sample(frac=sam, replace=False, axis=0)
        df_sample.to_sql(database, con=engine, if_exists='append', index=False, chunksize=100000)
    except:
        pass


def layered_by_k_means():
    sample_sum = 788
    database = 'dbscan_result'
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
    # 取出每层的数据
    df_list, zero_data = spilt_data_by_layer(layer, df)
    sampling_data(engine, df_list, data_sum, sample_sum, zero_data,database)


# 分为0，非0两层
def two_layer():
    sample_sum = 788
    database = 'dbscan_result'
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
    df_list = [z_df, fz_df]
    zero_data = DataFrame()
    sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database)


# 从非0数据项直接抽取
def without_zero():
    sample_sum = 12
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
    df_sample = fz_df.sample(frac=sample_sum / len(fz_df), replace=False, axis=0)
    print(df_sample)
    print(df_sample.describe())
    df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


# 把非0进行聚类后分层
def without_z_layer():
    sample_sum = 12
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
        molecular = sample_sum * w * std
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


def DBSCAN(same_data, Eps=0.003, MinPts=4):
    class_list = []
    f_core = set()  # 存放不是核心点
    y_core = {}  # 存放是核心点
    data = same_data.keys()
    t_core = []
    for core in data:  # 遍历所有找出核心点
        t_core.clear()
        for score in data:  # 找出核心点在Eps邻域中的点
            same_point = same_data[score]  # 相同点数量
            if round(abs(core - score), 4) <= Eps and abs(core - score) != 0:
                for i in range(same_point):
                    t_core.append(score)
        if len(t_core) >= MinPts:  # 是核心点
            t_core.append(core)
            s = t_core.copy()
            y_core[core] = set(s)
        else:
            f_core.add(core)
    ct_cores = y_core.keys()  # 核心点集
    ct_cores = list(set(ct_cores))
    # 聚类
    print('Clustering')
    while len(ct_cores) != 0:
        core = ct_cores[0]
        del ct_cores[0]
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
        ct_cores = list(set(ct_cores) - now_class)
    return class_list


def Cluster_by_DBSCAN():
    print('DBSCAN START')
    database = 'dbscan_result'
    sample_sum = 788
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.dbscan_result;'
    cursor.execute(sql)
    sql = 'select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        if float(i[0]) == 0:
            continue
        same_data[float(i[0])] = int(i[1])
    layer = DBSCAN(same_data)
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    df_list, zero_data = spilt_data_by_layer(layer, df)
    data_sum = len(sql_result)
    sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database)


def get_tup_index(lis):
    ind = []
    for i in lis:
        ind.append(i[0])
    return ind


def get_core_distance(core_points, MinPts):
    core_distance = {}
    for key in core_points.keys():
        t = sorted(core_points[key], key=lambda x: x[1])
        core_distance[key] = t[MinPts][1]  # 核心距离那个点
    return core_distance


def get_reach_distance(core_points, cdistance):
    reach_distance = {}
    distance_list = []
    for key in core_points.keys():
        cd = cdistance[key]
        distance_list.clear()
        for i in core_points[key]:
            if cd > i[1]:
                distance_list.append(cd)
            else:
                distance_list.append(i[1])
        reach_distance[key] = min(distance_list)
    return reach_distance


def insert_point(result_list, sorted_list, point_friends, core_d, list_ind, reach_distance):
    for p in point_friends:
        if p[0] not in result_list:
            rd = max(core_d, p[1])
            # 修改可达距离
            try:
                pre_rd = reach_distance[p[0]]
                if rd < pre_rd:
                    reach_distance[p[0]] = rd
            except:
                reach_distance[p[0]] = rd
            # 插入队列操作
            try:
                k = list_ind.index(p[0])
                distance = sorted_list[k][1]
                if distance > rd:  # 新距离小替换
                    sorted_list[k] = (p[0], rd)
            except:
                sorted_list.append((p[0], rd))


def OPTICS_Cluster(result, reach_distance, core_distance, Eps):
    layer = []
    t_list = []
    for point in result:
        rd = reach_distance[point]
        cd = core_distance[point]
        if rd > Eps or rd == 999:
            if cd != -999 and cd <= Eps:
                t = t_list.copy()
                layer.append(t)
                t_list.clear()
                t_list.append(point)
        else:
            t_list.append(point)
        '''
        if rd <= Eps:
            t_list.append(point)
        else:
            if cd <= Eps:
                l = t_list.copy()
                layer.append(l)
                t_list.clear()
                t_list.append(point)
        '''
    t = t_list.copy()
    layer.append(t)
    del layer[0]
    return layer


def OPTICS(same_data, Eps=0.003, MinPts=4):
    f_core = set()  # 存放不是核心点
    y_core = {}  # 存放是核心点
    reach_distance = {}
    data = same_data.keys()
    for core in data:  # 遍历所有找出核心点
        t_core = []
        for score in data:  # 找出核心点在Eps邻域中的点
            same_point = same_data[score]  # 有多少个相同点
            if round(abs(core - score), 4) <= Eps and abs(core - score) != 0:
                tup = (score, round(abs(core - score), 3))
                for i in range(same_point):
                    t_core.append(tup)
        if len(t_core) >= MinPts:  # 是核心点
            tup = (core, 0)
            t_core.append(tup)
            s = t_core.copy()
            y_core[core] = s
        else:
            f_core.add(core)
    core_distance = get_core_distance(y_core, MinPts)
    # 核心点的直接密度可达点去重
    for key in y_core.keys():
        y_core[key] = set(y_core[key])

    core_points = y_core.keys()
    core_list = list(y_core.keys())  # 核心点集
    point_list = core_list + list(f_core)  # 所有点集
    #  初始化所有点的核心距离，可达距离。核心距离小，可达距离大
    for p in point_list:
        reach_distance[p] = 999
        try:
            core_distance[p]
        except:
            core_distance[p] = -999
    extend_set = set()  # 存放拓展了的点
    result_list = []  # 结果队列
    sorted_list = []  # 有序队列
    while len(point_list) != 0:
        point = point_list[0]
        if point not in extend_set:
            del point_list[0]
            extend_set.add(point)
            result_list.append(point)
        else:
            continue
        if point not in core_list:
            continue
        point_friends = list(y_core[point])  # 取出直接密度可达点
        ind = get_tup_index(sorted_list)
        insert_point(result_list, sorted_list, point_friends, core_distance[point], ind, reach_distance)
        sorted_list.sort(key=lambda x: x[1])
        while len(sorted_list) != 0:
            ind = get_tup_index(sorted_list)  # 获取元祖的xy中的x顺序列
            min_d_point = sorted_list[0]  # 取出可达距离最短点
            del sorted_list[0]
            if min_d_point[0] not in result_list:  # 加入到结果队列
                k = point_list.index(min_d_point[0])
                del point_list[k]
                extend_set.add(min_d_point[0])
                result_list.append(min_d_point[0])
            if min_d_point[0] in core_points:  # 是核心点进行拓展
                point_friends = list(y_core[min_d_point[0]])
                insert_point(result_list, sorted_list, point_friends, core_distance[point], ind, reach_distance)
                sorted_list.sort(key=lambda x: x[1])
    # 聚类
    layer = OPTICS_Cluster(result_list, reach_distance, core_distance, Eps)
    return layer


def Cluster_by_OPTICS():
    print('OPTICS START')
    sample_sum = 788
    database = 'optics_result'
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.optics_result;'
    cursor.execute(sql)
    sql = 'select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        if float(i[0]) == 0:
            continue
        same_data[float(i[0])] = int(i[1])
    layer = OPTICS(same_data)
    for i in layer:
        print(i)
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    df_list, zero_data = spilt_data_by_layer(layer, df)
    data_sum = len(sql_result)
    sampling_data(engine, df_list, data_sum, sample_sum, zero_data, database)


def sample_by_rate(engine, df_list, data_sum, sample_sum, zero_data):
    # print(len(zero_data), data_sum, sample_sum)
    for df in df_list:
        specimen = len(df) / data_sum * sample_sum / len(df)
        # print(specimen)
        df_sample = df.sample(frac=specimen, replace=False, axis=0)
        df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)
    sam = len(zero_data) / data_sum * sample_sum / len(zero_data)
    # print(sam)
    df_sample = zero_data.sample(frac=sam, replace=False, axis=0)
    df_sample.to_sql('small_test', con=engine, if_exists='append', index=False, chunksize=100000)


def sample_by_layer_rate():
    print('RATE_Layer START')
    sample_sum = 788
    engine = create_engine('mysql+pymysql://root:@localhost:3308/unknown_data', encoding='utf8')
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'TRUNCATE TABLE unknown_data.small_test;'
    cursor.execute(sql)
    sql = 'select score, count(score) from unknown_data.data3 where `index`=22021001101410011321 group by score;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        if float(i[0]) == 0:
            continue
        same_data[float(i[0])] = int(i[1])
    layer = OPTICS(same_data)
    sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    cursor.execute(sql)
    sql_result = cursor.fetchall()
    df = DataFrame(sql_result, columns=['score']).astype('float')
    df_list, zero_data = spilt_data_by_layer(layer, df)
    data_sum = len(sql_result)
    sample_by_rate(engine, df_list, data_sum, sample_sum, zero_data)


def test():
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    wb = Workbook()
    excel = wb.active
    excel['A1'] = 'data_sum'
    excel['B1'] = 'stand'
    excel['C1'] = 'DBSCAN'
    excel['D1'] = 'OPTICS'
    excel['E1'] = 'RATE_Layer'
    count = 2
    datas = 1200
    for sample_s in range(18672, 28000, 778):
        print(sample_s)
        excel['A' + str(count)] = str(datas) + 'W'
        excel['B' + str(count)] = 0.000389
        Cluster_by_OPTICS(sample_s)
        sql = 'SELECT AVG(score) FROM unknown_data.optics_result;'
        cursor.execute(sql)
        result = cursor.fetchall()
        res = result[0][0]
        excel['D' + str(count)] = round(float(res), 6)
        Cluster_by_DBSCAN(sample_s)
        sql = 'SELECT AVG(score) FROM unknown_data.dbscan_result;'
        cursor.execute(sql)
        result = cursor.fetchall()
        res = result[0][0]
        excel['C' + str(count)] = round(float(res), 6)
        sample_by_layer_rate(sample_s)
        sql = 'SELECT AVG(score) FROM unknown_data.small_test;'
        cursor.execute(sql)
        result = cursor.fetchall()
        res = result[0][0]
        excel['E' + str(count)] = round(float(res), 6)
        datas = datas + 50
        count = count + 1
    wb.save('layer_test2.xlsx')
