import math
import time
import numpy as np
import sql_connect
from minisom import MiniSom
from pandas import DataFrame
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler


def som_cluster():
    global length, sample_sum
    sql = 'select value from unknown_data.air where locationId=' + str(number) + ';'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    length = len(result)
    t_list = []
    for i in result:
        t_list.append([i[0], 0])
    origin_data = np.array(t_list).astype(float)
    scalar = MinMaxScaler()
    scalar.fit(origin_data)
    train_data = scalar.transform(origin_data)
    size = math.ceil(np.sqrt(5 * np.sqrt(len(train_data))))
    som = MiniSom(size, size, 2, sigma=2, learning_rate=0.5,
                  neighborhood_function='bubble', activation_distance='chebyshev')
    som.pca_weights_init(train_data)
    som.train_random(train_data, 500, verbose=False)
    cluster = test_som(som, train_data, origin_data)
    sample_sum = int(length / data_sum * (data_sum * 0.05))
    return cluster


def test_som(som, data, origin_data):
    result = {}
    for point, origin_point in zip(data, origin_data):
        win_position = som.winner(point)
        try:
            result[win_position].append(origin_point[0])
        except:
            result[win_position] = [origin_point[0]]
    return result


def original_rate():
    res = {}
    for key in cluster.keys():
        cluster_data = cluster[key]
        sam_sum = len(cluster_data) / length * sample_sum
        res[key] = [len(cluster_data), int(sam_sum), 0]
    return res


def big_adjust():  # 总数抽多了， big层减少，small层增加
    global sample_dict
    min_keys = list(mean_dict.keys())
    max_keys = list(rev_mean_dict.keys())
    for max_key in rev_mean_dict.keys():  # 需要少抽
        if sample_dict[max_key][1] == 0:  # 这层已经没有抽了
            continue
        for min_key in mean_dict.keys():  # 需要多抽
            if sample_dict[min_key][1] == sample_dict[min_key][0]:  # 这层已经抽满了
                continue
            if max_key == min_key:
                break
            mean_diff = rev_mean_dict[max_key] - mean_dict[min_key]
            if mean_diff <= diff:  # 组合可行
                #  print(rev_mean_dict[max_key], mean_dict[min_key])
                #  print('big_adjust')
                k = round(diff / mean_diff)  # 调整个数
                # print(k)
                if sample_dict[min_key][0] - sample_dict[min_key][2] > k:  # 少层可以多抽
                    # print('小层可多抽')
                    sample_dict[min_key][1] = sample_dict[min_key][2] + k
                    #  print('min_key:{0}, past:{1}, now:{2}'.format(min_key, sample_dict[min_key][2], sample_dict[min_key][1]))
                    if sample_dict[max_key][2] >= k:  # 多层可以少抽
                        #  print('大层够少抽')
                        sample_dict[max_key][1] = sample_dict[max_key][2] - k
                        #  print('max_key:{0}, past:{1}, now:{2}'.format(max_key, sample_dict[max_key][2], sample_dict[max_key][1]))
                    else:  # 多层不可以少抽
                        # print('大层不够少抽')
                        t_key = max_key
                        res = k - sample_dict[max_key][2]
                        sample_dict[max_key][1] = 0
                        # print('max_key:{0}, past:{1}, now:{2}'.format(max_key, sample_dict[max_key][2], sample_dict[max_key][1]))
                        while res > 0:
                            next_ind = max_keys.index(t_key) + 1
                            t_key = max_keys[next_ind]
                            if sample_dict[t_key][2] >= res:  # 本层够减少
                                sample_dict[t_key][1] = sample_dict[t_key][2] + res
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                                res = 0
                            else:  # 本层不够减少
                                res -= sample_dict[t_key][2]
                                sample_dict[t_key][1] = 0
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                else:  # 少层不可以多抽
                    # print('小层不可多抽')
                    if sample_dict[max_key][2] >= k:  # 多层可以少抽
                        # print('大层够少抽')
                        sample_dict[max_key][1] = sample_dict[max_key][2] - k
                        res = k - sample_dict[min_key][0] - sample_dict[min_key][1]
                        # print('min_key:{0}, past:{1}, now:{2}'.format(min_key, sample_dict[min_key][2], sample_dict[min_key][1]))
                        t_key = min_key
                        while res > 0:
                            next_ind = min_keys.index(t_key) + 1
                            t_key = min_keys[next_ind]
                            if sample_dict[t_key][0] - sample_dict[t_key][2] >= res:
                                sample_dict[t_key][1] = sample_dict[t_key][2] + res
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                            else:
                                res -= sample_dict[t_key][0] - sample_dict[t_key][2]
                                sample_dict[t_key][1] = sample_dict[t_key][0]
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                    else:  # 多层不可以少抽
                        # print('大层不够少抽')
                        res = k - sample_dict[max_key][2]
                        sample_dict[max_key][1] = 0
                        # print('max_key:{0}, past:{1}, now:{2}'.format(max_key, sample_dict[max_key][2], sample_dict[max_key][1]))
                        t_key = max_key
                        while res > 0:
                            next_ind = max_keys.index(t_key) + 1
                            t_key = max_keys[next_ind]
                            if sample_dict[t_key][2] >= res:
                                sample_dict[t_key][1] = sample_dict[t_key][2] - res
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                                res = 0
                            else:
                                res -= sample_dict[t_key][2]
                                sample_dict[t_key][1] = 0
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                        res = k - sample_dict[min_key][0] - sample_dict[min_key][2]
                        sample_dict[min_key][1] = sample_dict[min_key][0]
                        # print('min_key:{0}, past:{1}, now:{2}'.format(min_key, sample_dict[min_key][2], sample_dict[min_key][1]))
                        t_key = min_key
                        while res > 0:
                            next_ind = min_keys.index(t_key) + 1
                            t_key = min_keys[next_ind]
                            if sample_dict[t_key][0] - sample_dict[t_key][2] >= res:
                                sample_dict[t_key][1] = sample_dict[t_key][2] + res
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                            else:
                                res -= sample_dict[t_key][0] - sample_dict[t_key][2]
                                sample_dict[t_key][1] = sample_dict[t_key][0]
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                return None


def small_adjust():  # 总数抽少了，small层减少，big层增加 (ok)
    global sample_dict
    min_keys = list(mean_dict.keys())
    max_keys = list(rev_mean_dict.keys())
    for min_key in mean_dict.keys():  # 需要少抽
        if sample_dict[min_key][1] == 0:  # 这层已经不抽了
            continue
        for max_key in rev_mean_dict.keys():  # 需要多抽
            if sample_dict[max_key][1] == sample_dict[max_key][0]:  # 抽满了这层
                continue
            if max_key == min_key:
                break
            mean_diff = rev_mean_dict[max_key] - mean_dict[min_key]
            if mean_diff <= diff:  # 组合可行
                # print('small_adjust')
                k = round(diff / mean_diff)  # 调整个数
                if k <= (sample_dict[max_key][0] - sample_dict[max_key][2]):  # （big）层可以多抽k个
                    # print('大层可多抽')
                    sample_dict[max_key][1] += k  # (big)层直接增加k个
                    # print('max_key:{0}, past:{1}, now:{2}'.format(max_key, sample_dict[max_key][2], sample_dict[max_key][1]))
                    if k <= sample_dict[min_key][2]:  # （small）层可以少抽
                        # print('小层可少抽')
                        sample_dict[min_key][1] = sample_dict[min_key][2] - k
                        # print('min_key:{0}, past:{1}, now:{2}'.format(min_key, sample_dict[min_key][2], sample_dict[min_key][1]))
                    else:  # （small）层不够少抽(small本层，直接不抽，不够的下一层减少)
                        # print('少层不够少抽')
                        sample_dict[min_key][1] = 0  # 少的那层不抽
                        # print('min_key:{0}, past:{1}, now:{2}'.format(min_key, sample_dict[min_key][2], sample_dict[min_key][1]))
                        res = k - sample_dict[min_key][2]  # 不够的往下一层要的个数
                        t_key = min_key
                        while res > 0:
                            next_ind = min_keys.index(t_key) + 1  # 找到下一个
                            t_key = min_keys[next_ind]
                            if sample_dict[t_key][0] - sample_dict[t_key][2] > res:  # 这层可以减少res个
                                sample_dict[t_key][1] = sample_dict[t_key][2] - res
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                                res = 0
                            else:
                                res -= sample_dict[t_key][0] - sample_dict[t_key][2]
                                sample_dict[t_key][1] = 0
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                    return None
                else:  # 不支持多抽 (big本层先抽满，然后剩余的给下一层增加)
                    # print('大层不够多抽')
                    if k <= sample_dict[min_key][2]:  # （small）层够给
                        # print('小层可少抽')
                        sample_dict[min_key][1] = sample_dict[min_key][2] - k
                        res = k - (sample_dict[max_key][0] - sample_dict[max_key][2])
                        sample_dict[max_key][1] = sample_dict[max_key][0]
                        # print('max_key:{0}, past:{1}, now:{2}'.format(max_key, sample_dict[max_key][2], sample_dict[max_key][1]))
                        t_key = max_key
                        while res > 0:
                            next_ind = max_keys.index(t_key) + 1
                            t_key = max_keys[next_ind]
                            if sample_dict[t_key][0] - sample_dict[t_key][2] >= res:  # 这层可以拿res个
                                sample_dict[t_key][1] = sample_dict[t_key][2] + res
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                                res = 0
                            else:  # 装不下res个
                                res -= sample_dict[t_key][0] - sample_dict[t_key][2]
                                sample_dict[t_key][1] = sample_dict[t_key][0]
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                    else:  # （small）层不够给
                        # print('小层不够少抽')
                        res = k - sample_dict[min_key][2]
                        sample_dict[min_key][1] = 0
                        # print('min_key:{0}, past:{1}, now:{2}'.format(min_key, sample_dict[min_key][2], sample_dict[min_key][1]))
                        t_key = min_key
                        while res > 0:  # (small)层先减少完k个
                            next_ind = min_keys.index(t_key) + 1
                            t_key = min_keys[next_ind]
                            if sample_dict[t_key][2] > res:
                                sample_dict[t_key][1] = sample_dict[t_key][2] - res
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                                res = 0
                            else:
                                res -= sample_dict[t_key][2]
                                sample_dict[t_key][1] = 0
                                # print('min_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                        res = k - sample_dict[max_key][2]
                        sample_dict[max_key][1] = sample_dict[max_key][0]  # 抽满
                        # print('max_key:{0}, past:{1}, now:{2}'.format(max_key, sample_dict[max_key][2], sample_dict[max_key][1]))
                        t_key = max_key
                        while res > 0:  # (big)层依次增加
                            next_ind = max_keys.index(t_key) + 1
                            t_key = max_keys[next_ind]
                            if sample_dict[t_key][0] - sample_dict[t_key][2] > res:
                                sample_dict[t_key][1] = sample_dict[t_key][2] + res
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                                res = 0
                            else:
                                res -= sample_dict[t_key][0] - sample_dict[t_key][2]
                                sample_dict[t_key][1] = sample_dict[t_key][0]  # 抽满
                                # print('max_key:{0}, past:{1}, now:{2}'.format(t_key, sample_dict[t_key][2], sample_dict[t_key][1]))
                    return None


def sampling():
    s = []
    res_df = DataFrame([])
    for key in cluster.keys():
        sam_rate = sample_dict[key][1] / sample_dict[key][0]
        if sam_rate > 1:
            sam_rate = 1
        df = DataFrame(cluster[key])
        try:
            sample = df.sample(frac=sam_rate, replace=False, axis=0)
        except:
            print(sam_rate)
            print(sample_dict[key])
        sample_dict[key][2] = len(sample)
        s.append((sample_dict[key][1], sample_dict[key][2]))
        res_df = res_df.append(sample)
    print(s)
    return res_df


def get_stand():
    sql = 'select avg(value) from unknown_data.air where locationId=' + str(number) + ';'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    return float(res[0][0])


if __name__ == '__main__':
    stop_sign = False
    sql_con = sql_connect.Sql_c()
    number = 2741
    stand = get_stand()
    data_sum = 10838742
    sample_sum = 2048
    length = 0
    cluster = som_cluster()
    mean_dict = {}  # 存放每层的均值
    stand_dict = {}  # 存放每层的方差
    for key in cluster.keys():
        arr = np.array(list(cluster[key]))
        if np.mean(arr) != 0:
            mean_dict[key] = np.mean(arr)
            stand_dict[key] = np.var(arr)
    sort_res = sorted(mean_dict.items(), key=lambda kv: kv[1])
    mean_dict.clear()
    for i in sort_res:
        mean_dict[i[0]] = i[1]
    sort_res = sorted(mean_dict.items(), key=lambda kv: kv[1], reverse=True)
    rev_mean_dict = {}
    for i in sort_res:
        rev_mean_dict[i[0]] = i[1]
    sample_dict = original_rate()  # 存放每层初始应该抽取多少 第一个是该层数据总量，第二个是该抽多少，第三个是实际抽了多少（存在多一个可能）
    res_list = []
    adjust = []
    for i in range(200):
        sample_df = sampling()
        sample_mean = sample_df.mean()
        error = abs(sample_mean - stand) / abs(stand) * 100  # 计算本次误差
        print(error)
        res_list.append(error)

        if error[0] < 0.01:
            adjust.append(2)
            continue

        diff = stand * sample_sum - sample_mean[0] * sample_sum  # 计算本次误差距离结果差距多少
        # 调整
        if diff > 0:  # 数值低，高数值抽少了
            print(0)
            small_adjust()
            adjust.append(0)
        else:  # 数值高，低数值抽少了
            print(1)
            diff = 0 - diff
            big_adjust()
            adjust.append(1)
    index = [i for i in range(1, 201)]
    plt.figure(figsize=(20, 8))
    plt.plot(index, res_list)
    # plt.show()
    plt.plot(index, adjust)
    plt.show()
    print(sum(res_list) / len(res_list))