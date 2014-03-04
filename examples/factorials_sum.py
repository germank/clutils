#!/usr/bin/env python

from clutils.building_blocks import create_map_reduce_pipeline
import os
from factorial_defs import factorial, multiarg_sum

def main():
    args = [10,100,1000]
    #IMPORTANT: the working path must be in a shared directory
    p = create_map_reduce_pipeline(os.path.expanduser('~/tmp/factorial'), 
                                 factorial, multiarg_sum, args)
    #In real cases, don't use the default config!
    p.run(debug=False, resume=False, config=None)
    
    print "Sum:",  p['output'].read().next()

    
if __name__ == '__main__':
    main()