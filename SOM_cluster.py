'''
利用som网络对输入的数据进行聚类
数据在输入时会从一维填0补充为二维
然后利用官方som库进行聚类返回聚类结果
进行抽样
'''
import time
import numpy as np
import sql_connect
from minisom import MiniSom
from pandas import DataFrame
from sklearn.preprocessing import MinMaxScaler


def my_test():
    # sql = 'select value from unknown_data.air where locationId=' + str(number) + ';'
    # sql = 'select score from unknown_data.data3 where `index`=22021001101410011321;'
    sql = 'select value from unknown_data.air;'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    t_list = []
    for i in result:
        t_list.append([i[0], 0])
    origin_data = np.array(t_list).astype(float)
    scalar = MinMaxScaler()
    scalar.fit(origin_data)
    train_data = scalar.transform(origin_data)
    # size = math.ceil(np.sqrt(1 * np.sqrt(len(train_data))))
    size = 20
    som = MiniSom(size, size, 2, sigma=5, learning_rate=0.5,
                  neighborhood_function='bubble', activation_distance='chebyshev')
    som.pca_weights_init(train_data)
    t1 = time.perf_counter()
    som.train_random(train_data, int(len(train_data) / 2), verbose=False)
    t2 = time.perf_counter()
    print(t2 - t1)
    cluster = test_som(som, train_data, origin_data)
    return cluster, len(origin_data)


def test_som(som, data, origin_data):
    result = {}
    for point, origin_point in zip(data, origin_data):
        win_position = som.winner(point)
        try:
            result[win_position].append(origin_point[0])
        except:
            result[win_position] = [origin_point[0]]
    return result


def main():
    sql_con = sql_connect.Sql_c()
    accuracy_list = []
    # data_sum = 10838742
    data_sum = 1
    number = 7990
    sql = 'select avg(value) from unknown_data.air where locationId=' + str(number) + ';'
    sql_con.cursor.execute(sql)
    res = sql_con.cursor.fetchall()
    stand = float(res[0][0])
    stand = 0.000389
    cluster, length = my_test()
    for key in cluster.keys():
        print(set(cluster[key]))
    for rate in range(5, 51, 5):
        sample = data_sum * (rate / 100)
        rem = (length / data_sum * sample) % 1
        if rem < 0.5:
            sample_sum = int(length / data_sum * sample)
        else:
            sample_sum = int(length / data_sum * sample) + 1
        errors = 0
        for j in range(1000):
            sample_df = DataFrame([])
            for key in cluster.keys():
                cluster_data = cluster[key]
                df = DataFrame(cluster_data)
                sam = len(cluster_data) / length * sample_sum / len(cluster_data)
                sample = df.sample(frac=sam, replace=False, axis=0)
                sample_df = sample_df.append(sample)
            error = abs(sample_df.mean() - stand) / stand
            errors += abs(error)
        accuracy = errors / 1000 * 100
        print('accuracy')
        print(accuracy)
        accuracy_list.append(accuracy)
    result_data = DataFrame(accuracy_list)
    result_data.to_csv('som.csv')


if __name__ == '__main__':
    sql_con = sql_connect.Sql_c()

    my_test()
