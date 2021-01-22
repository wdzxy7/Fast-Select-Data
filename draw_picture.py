from decimal import Decimal
import xlrd
import matplotlib.pyplot as plt
import networkx
from pandas import DataFrame
import pandas as pd


def get_data():
    data = pd.read_excel(time_path, sheet_name='Sheet1')
    df1 = DataFrame(data)
    df1.columns = proportion_list
    return df1


def draw_picture1(x_data):
    index = x_data.index.values
    print(index)
    plt.title(distribution)
    plt.plot(x_data, color='red', linewidth=2.0, marker='*')
    plt.show()


def get_sample_time():
    data = pd.read_excel(accuracy_path, sheet_name=name)
    df2 = DataFrame(data)
    # 100%数据行
    stand_cost = df2.iloc[0]
    result = []
    # 迭代之后每一行
    for i in range(1, 21):
        sample_data = df2.iloc[i]
        sub = sample_data - stand_cost
        sub = abs(sub)
        div = sub / stand_cost
        result.append(div)
    # 把求完误差率的数据转换为dataframe 没有100%数据
    df3 = DataFrame(result)
    return df3.sum(axis=1) / 12


if __name__ == '__main__':
    plt.figure(figsize=(20, 8))
    distribution = 'exponential'
    # distribution = 'random'
    # distribution = 'normal'
    # distribution = 'poisson'
    proportion_list = ['0.00005%', '0.0001%', '0.00015%', '0.0002%', '0.00025%', '0.0003%', '0.00035%', '0.0004%',
                       '0.00045%', '0.0005%', '0.00055%', '0.0006%', '0.00065%', '0.0007%', '0.00075%', '0.0008%',
                       '0.00085%', '0.0009%', '0.00095%', '0.001%']
    accuracy_path = distribution + '_data_accuracy.xlsx'
    time_path = distribution + '_data_create_time.xlsx'
    df = get_data()
    x_data = []
    # 迭代每一列求出平均值
    for proportion in proportion_list:
        average = df[proportion].mean()
        x_data.append(average)
    # 画时间变化图
    df = DataFrame(x_data)
    df.index = proportion_list
    print('x_data')
    # print(df)
    draw_picture1(df)

    plt.figure(figsize=(20, 8))
    df_list = []
    # 计算每一个sample中的误差率
    for num in range(1, 11):
        name = 'Sheet' + str(num)
        df = get_sample_time()
        df_list.append(df)
    df = df_list[0]
    # 把每一个sample的误差率求和然后求平均值
    for num in range(1, len(df_list)):
        df = df + df_list[num]
    print(df)
    df = df / 10
    # df.drop(0, axis=0, inplace=True)
    df.index = proportion_list
    print('accuracy')
    print(df)
    draw_picture1(df)