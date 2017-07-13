import os

import pandas as pd
import dask
import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/pradap/profile_joins')

from dmagellan.blocker.attrequivalence.attr_equiv_blocker import AttrEquivalenceBlocker

datapath = "../datasets/"
# A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
# B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)

A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)
A = A.sample(50000)
B = B.sample(50000)

print('Reading the files done')
ab = AttrEquivalenceBlocker()
C = ab.block_tables(A, B, 'id', 'id', 'year', 'year', nltable_chunks=2,
                    nrtable_chunks=2,
                    compute=True, scheduler=dask.get, show_progress=False
                    )
print(len(C))
from runtime.blocker.overlap_blocker import profile_candset

est = profile_candset(C, A, B, "l_id", "r_id", "id", "id", 'title', 'title', compute=True,
                      grid_params={'nchunks':[1, 2, 4]})
print(est)
import time
t1 = time.time()
D = ab.block_candset(C, A, B, "l_id", "r_id", "id", "id", 'year', 'year', compute=True,
                    nchunks=4)
print(time.time()-t1)
t1 = time.time()
D = ab.block_candset(C, A, B, "l_id", "r_id", "id", "id", 'year', 'year', compute=True,
                    nchunks=2)
print(time.time()-t1)

t1 = time.time()
D = ab.block_candset(C, A, B, "l_id", "r_id", "id", "id", 'year', 'year', compute=True,
                    nchunks=1)
print(time.time()-t1)
# print(len(D))
