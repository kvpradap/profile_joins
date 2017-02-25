from memory_profiler import memory_usage
from processify import processify
from collections import OrderedDict
import time
import pandas as pd
from scipy.interpolate import InterpolatedUnivariateSpline
import math

@processify
def _profile_memory(proc, repeat=3, interval=0.1, timeout=None, include_children=True):
    mem_usage = []
    counter = 0
    baseline = memory_usage()[0]

    import gc
    gc.collect()

    while counter < repeat:
        counter += 1
        tmp = memory_usage(proc, timeout=timeout, interval=interval,
                           include_children=include_children, max_usage=True)
        mem_usage.append(tmp[0])

    s = 0
    for m in mem_usage:
        s += (m - baseline)


    return { 'MaxMem': max(mem_usage) - baseline,
             'MinMem': min(mem_usage) - baseline,
             'AvgMem':(s) / float(repeat)
            }






def mem_profile(proc, vary_proportion_b = [0.1, 0.2, 0.3, 0.4], repeat = 3, interval=0.2):
    len_b = len(proc[1][1]) # This will be the index of table B
    len_a = len(proc[1][0])
    B = proc[1][1]
    stats_list = []
    for prop in vary_proportion_b:
        n = math.ceil(prop*len_b)
        # print(n)
        tmp = (proc[0], (proc[1][0], B.sample(n),), proc[2])
        d = _profile_memory ((tmp), repeat=repeat, interval=interval)
        d['Num Tuples in A'] = len_a
        d['Num Tuples in B'] = n
        d['Computed'] = True
        stats_list.append(d)
        time.sleep(1)

    stats = pd.DataFrame(stats_list)
    x, y, z = list(stats['Num Tuples in A']), \
              list(stats['Num Tuples in B']), list(stats['MaxMem'])
    interpolation_function = InterpolatedUnivariateSpline(y, z, k=1)
    estimated_memory_usage = interpolation_function(len_b) + 0.0
    print_stats(stats_list, estimated_memory_usage, len_b)




    return True

def print_stats(stats_list, est, len_b):
    for stats in stats_list:
        n_a, n_b, max_mem = stats['Num Tuples in A'], stats['Num Tuples in B'], stats['MaxMem']
        print('Computed max. memory usage( len(A): ' + str(n_a) + ', len(B): ' + str(n_b) +' ) = '
              + str(math.ceil(max_mem)) + ' MB')
    print()
    print('Estimated memory usage( len(A): ' + str(n_a) + ', len(B): ' + str(len_b) +') = '
              + str(math.ceil(est)) + ' MB')


def mur():
    pass









