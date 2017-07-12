import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/pradap/profile_joins')
#from memory.memory_profiler_joins import *
from runtime.runtime_profiler_joins import *
import py_stringsimjoin as ssj
import py_stringmatching as sm

A, B = ssj.load_books_dataset()
tok = sm.WhitespaceTokenizer()
proc = (ssj.jaccard_join, (A, B,), {
        'l_key_attr':'ID',
        'r_key_attr':'ID',
        'l_join_attr':'Title',
        'r_join_attr':'Title',
        'tokenizer':tok,
        'threshold':0.1,
        'show_progress':False,
        'n_jobs':1
    })

d = time_profile(proc, vary_proportion_b=[0.3, 0.6],  repeat=3)
# res = ssj.jaccard_join(A, B, 'ID', 'ID', 'Title', 'Title', tokenizer=tok,
#                        threshold=0.1, n_jobs=-1)
