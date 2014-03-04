import os
import errno
from collections import Mapping
from itertools import chain


#auxiliary functions
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise
        

def dict_merge(a, b):
    '''recursively merges dict's. not just simple a['key'] = b['key'], if
    both a and have a key who's value is a dict then dict_merge is called
    on both values and the result stored in the returned dictionary. 
    If, on the contrary, they share a key, but the value is not a dict, then
    dict a values are preferred'''
    if not isinstance(b, dict):
        return b
    result = b
    for k, v in a.iteritems():
        if k in result and isinstance(result[k], dict):
                result[k] = dict_merge(v, result[k])
        else:
            result[k] = v
    return result

same = lambda x:x  # identity function
add = lambda a,b:a+b
_tuple = lambda x:(x,)  # python actually has coercion, avoid it like so

def dict_flatten(dictionary, keyReducer=add, keyLift=_tuple, init=()):

    # semi-lazy: goes through all dicts but lazy over all keys
    # reduction is done in a fold-left manner, i.e. final key will be
    #     r((...r((r((r((init,k1)),k2)),k3))...kn))

    def _flattenIter(pairs, _keyAccum=init):
        atoms = ((k,v) for k,v in pairs if not isinstance(v, Mapping))
        submaps = ((k,v) for k,v in pairs if isinstance(v, Mapping))
        def compress(k):
            return keyReducer(_keyAccum, keyLift(k))
        return chain(
            (
                (compress(k),v) for k,v in atoms
            ),
            *[
                _flattenIter(submap.items(), compress(k))
                for k,submap in submaps
            ]
        )
    return dict(_flattenIter(dictionary.items()))

