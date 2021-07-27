import sql_connect
import numpy as np
import som_code as SOM
from sklearn.preprocessing import MinMaxScaler


def test_som(som, data, origin_data):
    result = {}
    for point, origin_point in zip(data, origin_data):
        win_position = som.winner(point)
        try:
            result[win_position].append(origin_point[0])
        except:
            result[win_position] = [origin_point[0]]
    return result


if __name__ == '__main__':
    # 加载数据
    sql_con = sql_connect.Sql_c()
    sql = 'select value from unknown_data.air where locationId=63094;'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    # 维度转换
    t_list = []
    for i in result:
        t_list.append([i[0], 0])
    origin_data = np.array(t_list).astype(float)
    # 归一化
    scalar = MinMaxScaler()
    scalar.fit(origin_data)
    train_data = scalar.transform(origin_data)
    size = 16
    som = SOM.MiniSom(size, size, 2, sigma=4, learning_rate=0.5, neighborhood_function='bubble', activation_distance='euclidean')
    som.pca_weights_init(train_data)
    som.train_batch(train_data, 500, verbose=False)
    cluster = test_som(som, train_data, origin_data)
    for key in cluster.keys():
        print(key, set(cluster[key]))
