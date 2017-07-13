# coding=utf-8

import time
import math
import pandas as pd
from scipy.interpolate import InterpolatedUnivariateSpline
from scipy.interpolate import interp1d


class Timer(object):
    def __init__(self):
        self.interval = 0

    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self,  *args):
        self.end = time.clock()
        self.interval = self.end - self.start


def profile_time(proc, repeat):
    time_usage = []

    counter = 0
    if callable(proc):
        proc = (proc, (), {})

    func, args, kwargs = None, None, None
    if isinstance(proc, (list, tuple)):
        if len(proc) == 1:
            func, args, kwargs = (proc[0], (), {})
        elif len(proc) == 2:
            func, args, kwargs = (proc[0], proc[1], {})
        elif len(proc) == 3:
            func, args, kwargs = (proc[0], proc[1], proc[2])
        else:
            raise ValueError

    while counter < repeat:
        counter += 1
        try:
            with Timer() as timer:
                _ = func(*args, **kwargs)
        finally:
            time_usage.append(timer.interval)

    return min(time_usage)


def profiler_candset(proc, sample_proportions=[0.1, 0.2, 0.3], repeat=1):
    candset = proc[1]
    len_candset = len(candset)
    stats = []

    for p in sample_proportions:
        num_tuples = int(math.ceil(p * len_candset))
        proc_tuple = (proc[0], (candset.sample(num_tuples, random_state=0),), proc[2])
        stats_dict = {}

        t = profile_time((proc_tuple), repeat=repeat)

        stats_dict['Num Tuples in Candset'] = num_tuples
        stats_dict['Computed'] = True
        stats_dict['Time Taken'] = t
        stats.append(stats_dict)

        time.sleep(3)

    stats_df = pd.DataFrame(stats)

    # get y and z
    y = list(stats_df['Num Tuples in Candset'])
    z = list(stats_df['Time Taken'])

    # interpolate
    interpolate_function = InterpolatedUnivariateSpline(y, z, k=3)
    print(dict(zip(y, z)))


    estimated_time = interpolate_function(len_candset) + 0.0

    return estimated_time, stats_df, stats


def printstats_candset(stats, estimated_time, len_candset):
    for s in stats:
        num_tuples = s['Num Tuples in Candset']
        max_time = s['Time Taken']
        print('actual | num. tuples: {0} | time: {1} (s)'.format(num_tuples, max_time))
    print('estimated | num. tuples: {0} | time: {1} (s)'.format(len_candset, estimated_time))



