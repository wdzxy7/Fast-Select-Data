import matplotlib.pyplot as plt
from pandas import DataFrame
import pandas as pd


def get_data(file_path, proportion_list, sheet):
    data = pd.read_excel(file_path, sheet_name=sheet)
    df1 = DataFrame(data)
    df1.columns = proportion_list
    return df1


def draw_picture1(x_data, distribution):
    index = x_data.index.values
    print(index)
    plt.title(distribution)
    plt.plot(x_data, color='red', linewidth=2.0, marker='*')
    plt.show()


def get_sample_time(accuracy_path, name):
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


def draw_create_data():
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
    df = get_data(time_path, proportion_list, 'Sheet1')
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
    draw_picture1(df, distribution)

    plt.figure(figsize=(20, 8))
    df_list = []
    # 计算每一个sample中的误差率
    for num in range(1, 11):
        name = 'Sheet' + str(num)
        df = get_sample_time(accuracy_path, name)
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
    draw_picture1(df, distribution)


def read_csv(file_path):
    data = pd.read_csv(file_path)
    df = DataFrame(data)
    return df


# 把100以上数据，强制转化为100
def change(x):
    if x > 100:
        return 100
    else:
        return x


def draw_unknown_data(data_type, table_sum):
    df = DataFrame()
    # 读取csv文件
    for i in range(1, table_sum + 1):
        file_path = data_type + '_Clustering_accuracy' + str(i) + '.csv'
        data = read_csv(file_path)
        if i == 1:
            df = data
        else:
            df = df + data
    df = df / table_sum
    # 便于画图把误差超过100的规定为100 使用change函数
    df['K-MEANS'] = df['K-MEANS'].apply(lambda x: change(x))
    df['DBSCAN'] = df['DBSCAN'].apply(lambda x: change(x))
    df['OPTICS'] = df['OPTICS'].apply(lambda x: change(x))
    df['RANDOM'] = df['RANDOM'].apply(lambda x: change(x))
    print(df)
    df.plot(figsize=(20, 8))
    plt.show()


def draw_air():
    data = DataFrame()
    for percentage in range(2, 11, 2):
        path = 'result/' + str(percentage) + '%/incline' + str(percentage) + '%.csv'
        df = read_csv(path)
        data = data.append(df, ignore_index=True)
    data.drop('Unnamed: 0', axis=1, inplace=True)
    data.plot(figsize=(20, 8))
    print(data)
    plt.show()


def k_to_k_means():
    df_dict = {}
    df = DataFrame()
    for k in range(1, 11):
        df_dict[k] = []
        for j in range(1, 11):
            file_path = 'result/k-means/k' + str(k) + '_Clustering_accuracy' + str(j) + '.csv'
            data = read_csv(file_path)
            if j == 1:
                df = data
            else:
                df = df + data
        df = df / 10
        df_dict[k] = df.copy(deep=True)
    df_list = []
    for key in df_dict.keys():
        df_dict[key].columns = [key]
        df_list.append(df_dict[key])
    df = pd.concat(df_list, axis=1)
    print(df)
    df.plot(figsize=(20, 8))
    plt.show()


def draw_statistics():
    file_path = 'statistics_result.csv'
    df = read_csv(file_path)
    df.drop(['all_k_means_random', 'group_k_means_random', 'group_dbscan_random', 'group_optics_random', 'group_proportion_k_means_random'], axis=1, inplace=True)
    df.plot(figsize=(20, 8))
    plt.show()


if __name__ == '__main__':
    # draw_statistics()
    # k_to_k_means()
    draw_unknown_data('incline', 10)
    # draw_air()