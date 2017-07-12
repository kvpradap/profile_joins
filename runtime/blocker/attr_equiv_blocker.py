import sys
import dmagellan as dm
from dmagellan.blocker.attrequivalence import attr_equiv_blocker
from runtime import utils
from dask import threaded
from pyprind import ProgBar
def profile_candset(candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                      r_key, l_block_attr, r_block_attr, nchunks=1,
                      scheduler=threaded.get,
                      num_workers=None, cache_size=1e9, compute=False,
                      show_progress=False, grid_params={}
                    ):
    # call the profiler command with different sample proportions

    d = get_args_dict(ltable, rtable, fk_ltable, fk_rtable, l_key,
                      r_key, l_block_attr, r_block_attr,
                      scheduler,
                      num_workers, cache_size, compute)
    ab = attr_equiv_blocker.AttrEquivalenceBlocker()


    estimated_times = []
    # pbar = ProgBar(len(grid_params['nchunks']))
    for chunk_size in grid_params['nchunks']:
        print(chunk_size)
        # pbar.update()
        d['nchunks']= chunk_size
        proc = (ab.block_candset, (candset), d)
        estimated_time, _, _ = utils.profiler_candset(proc, repeat=1)
        estimated_times.append((chunk_size, estimated_time))
    return estimated_times




def get_args_dict(ltable, rtable, fk_ltable, fk_rtable, l_key,
                      r_key, l_block_attr, r_block_attr,
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
    d['scheduler'] = scheduler
    d['num_workers'] = num_workers
    d['cache_size'] = cache_size
    d['compute'] = compute
    d['show_progress'] = False
    d['nchunks'] = 1

    return d