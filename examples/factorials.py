#!/usr/bin/env python

from clutils.building_blocks import create_parallel_pipeline
import os
from factorial_defs import factorial

def main():
    args = [[10],[100],[1000]]
    #IMPORTANT: the working path must be in a shared directory
    p = create_parallel_pipeline(os.path.expanduser('~/tmp/factorial'), 
                                 factorial, args)
    #In real cases, don't use the default config!
    p.run()
    
    for x,y in zip(args, p['output'].read()):
        print x, y

    
if __name__ == '__main__':
    main()
