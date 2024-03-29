import numpy as np
import pymysql
from sklearn.cluster import DBSCAN,KMeans,OPTICS
import cluster_sample_algorithm as st

# 调用官方库验证自卸算法是否正确

if __name__ == '__main__':
    connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
    cursor = connect.cursor()
    sql = 'select value from unknown_data.air where locationId=2536;'
    cursor.execute(sql)
    result = cursor.fetchall()
    data = []
    for i in result:
        data.append((float(i[0]), 0))
    arr = np.array(data)
    '''
    sql = 'select value, count(value) from unknown_data.air where locationId=63094 group by value;'
    cursor.execute(sql)
    res = cursor.fetchall()
    same_data = {}
    for i in res:
        same_data[float(i[0])] = int(i[1])
    dbscan_layer = st.DBSCAN(same_data, Eps=0.1001, MinPts=10)
    print('DBSCAN:')
    for i in dbscan_layer:
        print(sorted(i))
    '''

    cluster = DBSCAN(eps=0.3, min_samples=100).fit(arr)
    # cluster = KMeans(n_clusters=10).fit(arr)
    # cluster = OPTICS(min_samples=7, max_eps=0.0022).fit(arr)
    result = []
    for i, j in zip(cluster.labels_, data):
        tup = (i, j)
        result.append(tup)
    result = set(result)
    result_dict = {}
    for i in result:
        try:
            result_dict[i[0]].append(i[1][0])
        except:
            result_dict[i[0]] = []
            result_dict[i[0]].append(i[1][0])
    for key in result_dict:
        print(key, sorted(result_dict[key]))

'''
def find_neighbor(j, x, eps):
    N = list()
    for i in range(x.shape[0]):
        temp = np.sqrt(np.sum(np.square(x[j] - x[i])))  # 计算欧式距离
        if temp <= eps:
            N.append(i)
    return set(N)


def DBSCAN(X, eps, min_Pts):
    k = -1
    neighbor_list = []  # 用来保存每个数据的邻域
    omega_list = []  # 核心对象集合
    gama = set([x for x in range(len(X))])  # 初始时将所有点标记为未访问
    cluster = [-1 for _ in range(len(X))]  # 聚类
    for i in range(len(X)):
        neighbor_list.append(find_neighbor(i, X, eps))
        if len(neighbor_list[-1]) >= min_Pts:
            omega_list.append(i)  # 将样本加入核心对象集合
    omega_list = set(omega_list)  # 转化为集合便于操作
    core_data = []
    for i in omega_list:
        core_data.append(X[i][0])
    core_data = set(core_data)
    print('c_core:')
    print(sorted(list(core_data)))
    while len(omega_list) > 0:
        gama_old = copy.deepcopy(gama)
        j = random.choice(list(omega_list))  # 随机选取一个核心对象
        k = k + 1
        Q = list()
        Q.append(j)
        gama.remove(j)
        while len(Q) > 0:
            q = Q[0]
            Q.remove(q)
            if len(neighbor_list[q]) >= min_Pts:
                delta = neighbor_list[q] & gama
                deltalist = list(delta)
                for i in range(len(delta)):
                    Q.append(deltalist[i])
                    gama = gama - delta
        Ck = gama_old - gama
        Cklist = list(Ck)
        for i in range(len(Ck)):
            cluster[Cklist[i]] = k
        omega_list = omega_list - Ck
    return cluster
    
connect = pymysql.connect(host='localhost', port=3308, user='root', passwd='', db='', charset='utf8')
cursor = connect.cursor()
sql = 'select score from unknown_data.data3 where `index`=22021001101410011321 and score>0.02 order by score;'
cursor.execute(sql)
result = cursor.fetchall()
data = []
datas = []
for i in result:
    data.append((float(i[0]), 0))
    datas.append(float(i[0]))
X = np.array(data)
eps = 0.002
min_Pts = 8
begin = time.time()
C = DBSCAN(X, eps, min_Pts)
result = []
for i, j in zip(C, datas):
    tup = (i, j)
    result.append(tup)
result = set(result)
result_dict = {}
for i in result:
    try:
        result_dict[i[0]].append(i[1])
    except:
        result_dict[i[0]] = []
        result_dict[i[0]].append(i[1])
for key in result_dict:
    print(key, sorted(result_dict[key]))

end = time.time()
plt.figure()
plt.scatter(X[:, 0], X[:, 1], c=C)
plt.show()
'''
