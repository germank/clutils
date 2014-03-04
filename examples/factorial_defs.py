def factorial(n):
    r = 1
    for i in xrange(1,n):
        r = i*r
    return r     

def multiarg_sum(*args):
    return sum(args)