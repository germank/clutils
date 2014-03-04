#!/usr/bin/env python
import argparse
from clutils.pipeline import Pipeline
from count_words_defs import CountWords, SumCounts
import os

def main():
    arg_parser = argparse.ArgumentParser(description='Count words in target')
    arg_parser.add_argument('corpora', nargs='+')
    arg_parser.add_argument('-z', '--zipped', default=False, action='store_true')
    arg_parser.add_argument('-o', '--output', required=True)
    
    args = arg_parser.parse_args()
    
    
    #Create the pipeline using the argument sent with "-o" as
    #the working path 
    pln = Pipeline(args.output)
    #Create the summing module and use "sum" as the directory to put the output
    sum_module = SumCounts("sum")
    for corpus in args.corpora:
        #Create a counting module and use count as a directory to group all the
        # output of the counting modules with the name of the corpus file as
        # sub-directory
        count_module = CountWords("count", os.path.basename(corpus))
        #Set the arguments that will be sent to the "run" method
        count_module.set_args(corpus, args.zipped)
        #Connect the output of the count_module to the input of the summing module
        count_module['output'].connect_to(sum_module['input'])
        #Add the count_module
        pln.add_module(count_module)
    #Create a new stage (the sum_module needs to be run just after all the 
    #count modules have finished
    pln.add_stage()
    #Add the sum module
    pln.add_module(sum_module)

    #Create a configuration:
    #h_vmem: 5G of memory (at most) required
    #h_cpu: 1 hour (at most) will be used
    #'*': apply to all modules (pseudo wildcard pattern)
    config = {'*': {'h_vmem': '5G', 'h_cpu': '1:0:0'}}    
    #Run the pipeline
    #debug: specifies whether the job should be run locally
    #resume: don't re-run jobs for which the method "finished" returns true
    pln.run(debug=False, resume=True, config=config)

            
if __name__ == '__main__':
    main()