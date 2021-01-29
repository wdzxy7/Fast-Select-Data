from pandas import DataFrame
import math
point = [1,2,3,4,5,6]
k = point.index(5)
del point[k]
print(point)