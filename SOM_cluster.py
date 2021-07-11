import Som_Class
import numpy as np
import sql_connect
from sklearn.preprocessing import MinMaxScaler


def test():
    cluster_dict = {}
    for point, root in zip(test_data, data):
        min_distance = 99999
        j = 0
        for i in som.W:
            distance = point * i
            if distance < min_distance:
                min_distance = distance
                index = j
            j += 1
        try:
            cluster_dict[index].append(root)
        except:
            cluster_dict[index] = [root]
    return cluster_dict



def normal_data(t_data):
    scalar = MinMaxScaler()
    scalar.fit(t_data.reshape(-1, 1))
    t = scalar.transform(t_data.reshape(-1, 1))
    return_data = t.reshape(len(data))
    return return_data


if __name__ == '__main__':
    sql_con = sql_connect.Sql_c()
    sql = 'select value from unknown_data.air where locationId=63094;'
    sql_con.cursor.execute(sql)
    result = sql_con.cursor.fetchall()
    data = np.array(result).astype(float)
    print(data)
    print(len(data))
    som = Som_Class.SOM(data, lr=0.5, neighbor=50, train_times=10)
    som.train()
    test_data = normal_data(data)
    cluster = test()
    for key in cluster:
        print(key)