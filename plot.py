import matplotlib.pyplot as plt
import os
import sys
import numpy as np

path = sys.argv[1]

data = open(path, "r")

raw_data = data.read()

data.close()

lines = raw_data.split("\n")
test_data = [x.split("\t") for x in lines]

#Last item is "''"
test_data.pop()

tmp = [int(x[0]) for x in test_data]
num_tests = list(set(tmp))
num_tests.sort()

width = 0.20

fig, ax = plt.subplots()
n = np.arange(1, max(num_tests) + 1)

ax.set_title('Graph shit')
ax.set_ylabel('Time(s)')
ax.set_xlabel('# Depth')
ax.set_xticklabels(n)
ax.set_xticks(n + width)
plt.xlim(xmin=0, xmax=max(num_tests)+1.35)
#plt.xlim([0,n.size+0.35])

avg_list = []
avg2_list = []
std_list = []
std2_list = []

for idx, depth in enumerate(num_tests):

    vals = [float(x[1]) for x in test_data if int(x[0]) == depth]
    vals2 = [float(x[2]) for x in test_data if int(x[0]) == depth]

    total = sum(vals)
    avg = total/len(vals)
    std = np.std(vals)

    total2 = sum(vals2)
    avg2 = total2/len(vals2)
    std2 = np.std(vals2)

    avg_list.append(avg)
    avg2_list.append(avg2)

    std_list.append(std)
    std2_list.append(std2)


r = ax.bar(n, avg_list, width, color='cyan', yerr=std_list, error_kw=dict(ecolor='black', lw=2, capsize=5, capthick=1))
r2 = ax.bar(n + width, avg2_list, width, color='b', yerr=std2_list, error_kw=dict(ecolor='black', lw=2, capsize=5, capthick=1))

ax.legend((r[0], r2[0]), ('Neo4j', 'SQL'))
plt.savefig("test.pdf")

