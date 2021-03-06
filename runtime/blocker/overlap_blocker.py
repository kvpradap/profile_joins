import sys
sys.path.append('/scratch/pradap/python-work/dmagellan/dmagellan')
sys.path.append('/scratch/pradap/python-work/profiler_joins')
from dask import threaded
from dask import multiprocessing

from dmagellan.blocker.overlap import overlapblocker
import utils

def block_candset(candset, ltable, rtable, fk_ltable, fk_rtable, l_key, r_key, l_block_attr, r_block_attr,
                  rem_stop_words=False, q_val=None, word_level=True, overlap_size=1,
                  scheduler = multiprocessing.get, num_workers=None, cache_size=129, compute=False,
                  grid_params = {'nchunks':[1, 2, 4]}):

    # call the profiler command with different sample proportions
    d = get_args_dict_block_candset(ltable, rtable, fk_ltable, fk_rtable, l_key,
                      r_key, l_block_attr, r_block_attr,
                      rem_stop_words, q_val, word_level,
                      overlap_size,
                      scheduler,
                      num_workers, cache_size, compute)

    ob = overlapblocker.OverlapBlocker()
    random_state = 0
    sample_size = 0.1*len(candset)
    if sample_size > 5000:
        sample_size = 5000

    sample_candset = candset.sample(sample_size, random_state=random_state)
    for chunk_size in grid_params['nchunks']:
        print(chunk_size)
        d['nchunks'] = chunk_size
        proc = (ob.block_canset, (sample_candset), d)

        time_taken = execute_proc(proc, repeat=1)
        print(time_taken, chunk_size)







def get_args_dict_block_candset(ltable, rtable, fk_ltable, fk_rtable, l_key,
                  r_key, l_block_attr, r_block_attr,
                  rem_stop_words, q_val, word_level,
                  overlap_size,
                  scheduler, num_workers, cache_size, compute):
    d = {}
    d['ltable'] = ltable
    d['rtable'] = rtable
    d['fk_ltable'] = fk_ltable
    d['fk_rtable'] = fk_rtable
    d['l_key'] = l_key
    d['r_key'] = r_key
    d['l_block_attr'] = l_block_attr
    d['r_block_attr'] = r_block_attr
    d['rem_stop_words'] = rem_stop_words
    d['q_val'] = q_val
    d['word_level'] = word_level
    d['overlap_size'] = overlap_size
    d['scheduler'] = scheduler
    d['num_workers'] = num_workers
    d['cache_size'] = cache_size
    d['compute'] = compute
    d['show_progress'] = False
    d['nchunks'] = 1

    return d


def execute_proc(proc, repeat=1):
    return utils.profile_time(proc, repeat=repeat)