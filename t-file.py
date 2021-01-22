from pandas import DataFrame
import math
df = DataFrame([1.2, 1.2, 0.8])
l = [1.2, 1.2, 0.8]
s = sum(l)
print(s)
avg = s / 3
print(avg)
for i in range(len(l)):
    l[i] = l[i] - avg
    l[i] = l[i] * l[i]
    print(l[i])
print(l)
s = sum(l)
print(s)
s = s / 2
print(math.sqrt(s))
print(df.describe())


