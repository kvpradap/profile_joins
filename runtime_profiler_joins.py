from processify import processify
from collections import OrderedDict
import time
import pandas as pd
from scipy.interpolate import InterpolatedUnivariateSpline
import math


class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def _profile_time(proc, repeat=3):

    time_usage = []

    counter = 0
    if callable(proc):
        proc = (proc, (), {})

    if isinstance(proc, (list, tuple)):
        if len(proc) == 1:
            f, args, kw = (proc[0], (), {})
        elif len(proc) == 2:
            f, args, kw = (proc[0], proc[1], {})
        elif len(proc) == 3:
            f, args, kw = (proc[0], proc[1], proc[2])
        else:
            raise ValueError

    while counter < repeat:
        counter += 1
        try:
            with Timer() as t:
                res = f(*args, **kw)
        finally:
            time_usage.append(t.interval)
    return max(time_usage)

def time_profile(proc, vary_proportion_b = [0.1, 0.2, 0.3, 0.4], repeat = 3):
    len_b = len(proc[1][1]) # This will be the index of table B
    len_a = len(proc[1][0])
    B = proc[1][1]
    stats_list = []

    for prop in vary_proportion_b:
        n = math.ceil(prop*len_b)
        print(n)
        tmp = (proc[0], (proc[1][0], B.sample(n),), proc[2])
        stats_dict = {}
        d = _profile_time ((tmp), repeat=repeat)
        stats_dict['Num Tuples in A'] = len_a
        stats_dict['Num Tuples in B'] = n
        stats_dict['Computed'] = True
        stats_dict['TimeTaken'] = d
        print(d)
        print()
        stats_list.append(stats_dict)

        time.sleep(1)
    print()
    for d in stats_list:
        print(d)
        print()

    stats = pd.DataFrame(stats_list)
    print()
    print(stats)
    x, y, z = list(stats['Num Tuples in A']), \
                  list(stats['Num Tuples in B']), list(stats['TimeTaken'])
    print()
    print(y)

    print()
    print(z)


    interpolation_function = InterpolatedUnivariateSpline(y, z, k=1)
    estimated_time = interpolation_function(len_b) + 0.0
    print()
    print(estimated_time)
    print()
    print_stats(stats_list, estimated_time, len_b)
    return True

def print_stats(stats_list, est, len_b):
    for stats in stats_list:
        n_a, n_b, max_time = stats['Num Tuples in A'], stats['Num Tuples in B'], stats['TimeTaken']
        print('Computed max. time taken( len(A): ' + str(n_a) + ', len(B): ' + str(n_b) +' ) = '
              + str(math.ceil(max_time)) + ' seconds')
    print()
    print('Estimated time ( len(A): ' + str(n_a) + ', len(B): ' + str(len_b) +') = '
              + str(math.ceil(est)) + ' seconds')



