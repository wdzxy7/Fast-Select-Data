'''
自己复现的som代码，在计算欧式距离那里有点小问题别的没有大问题
感觉som还是比较时候二维，一维数据可以补0升二维进行计算聚类
'''
import math
import random
import numpy as np
from sklearn.preprocessing import MinMaxScaler


class SOM(object):
    def __init__(self, data, lr, train_times=200, neighbor=4):
        self.data = data
        self.W = None
        self.lr = lr
        self.static_neighbor = neighbor
        self.neighbor_size = neighbor
        self.train_times = train_times
        self.min_distance = None
        self.T = train_times / 2

    # 初始化权重矩阵
    def get_W(self):
        length = math.ceil(5 * np.sqrt(len(self.data)))
        self.W = np.random.rand(round(length) + 1)

    #  计算获胜节点 坐标
    def get_winner(self, point):
        self.min_distance = 99999
        j = 0
        index = 0
        for i in self.W:
            distance = math.sqrt((point - i) * (point - i))
            if distance < self.min_distance:
                self.min_distance = distance
                index = j
            j += 1
        return index

    # 更新权重矩阵
    def update_W(self, neighbor, point):
        update_rate = gaussian(len(neighbor), self.neighbor_size)
        for i, rate in zip(neighbor, update_rate):
            try:
                self.W[i] = self.W[i] + rate * self.lr * (point - self.W[i])
            except:
                continue

    # 根据领域找出邻居
    def get_neighbor(self, core):
        pace = 0
        neighbor_list = [core]
        while pace < self.neighbor_size:
            neighbor_list.append(core + pace)
            neighbor_list.insert(0, (core - pace))
            pace += 1
        return neighbor_list

    def update_lr(self, epoch):
        self.lr = self.lr / (1 + epoch / self.T)

    # 训练
    def train(self):
        self.get_W()
        self.normal_W()
        self.normal_data()
        for epoch in range(1, self.train_times):
            for point in self.data:
                winner = self.get_winner(point)
                neighbors = self.get_neighbor(winner)
                self.update_W(neighbors, point)
            self.update_lr(epoch)
            self.update_neighbor_size(epoch)
            print(self.neighbor_size)
        print(self.W)

    # 更新领域大小
    def update_neighbor_size(self, epoch):
        self.neighbor_size = self.neighbor_size / (1 + epoch / self.T)

    def normal_data(self):
        scalar = MinMaxScaler()
        scalar.fit(self.data.reshape(-1, 1))
        data = scalar.transform(self.data.reshape(-1, 1))
        self.data = data.reshape(len(data))

    def normal_W(self):
        scalar = MinMaxScaler()
        scalar.fit(self.W.reshape(-1, 1))
        data = scalar.transform(self.W.reshape(-1, 1))
        self.W = data.reshape(len(data))


# 计算领域内更新幅度
def gaussian(size, sigma):
    arr = np.arange(size)
    d = 2 * np.pi * sigma * sigma
    update_rate = np.exp(-np.power(arr - (size // 2 + 1), 2) / d)
    return update_rate
