import yaml
import collections
def nodenames(match_expr):
    import re
    nodes = ["compute-{0}-{1}".format(i,j) for i in [0,1] for j in
        [1,2,3,4,5,6,7,8,9]]
    return "|".join([n for n in nodes if re.match(match_expr, n)])


def replace_nodenames(d):
    if isinstance(d, collections.Mapping):
        for k,v in d.iteritems():
            if k == 'hosts':
                if isinstance(v, basestring):
                    v = nodenames(v)
                elif isinstance(v, list):
                    v = "|".join(v)
            else:
                v = replace_nodenames(v)
            d[k] = v
        return d
    else:
        return d
            
                
def load_config(yml_file):
    config = yaml.load(file(yml_file))
    config = replace_nodenames(config)
    return config