def nodenames(match_expr):
    import re
    nodes = ["compute-{0}-{1}".format(i,j) for i in [0,1] for j in
        [1,2,3,4,5,6,7,8,9]]
    return "|".join([n for n in nodes if re.match(match_expr, n)])

