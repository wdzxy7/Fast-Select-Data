from pandas import DataFrame
import math
ct_cores = [1,2,3,4]
m = [1,2,5]
t = list(ct_cores)
core = t[0]
del t[0]
print(set(ct_cores) - set(m))