#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Written (W) 2008-2013 Christian Widmer
# Copyright (C) 2008-2013 Max-Planck-Society


from pythongrid import KybJob, process_jobs

import logging
import time
import argparse
import fileinput
import sys
import pickle


def main():
    parser = argparse.ArgumentParser(description=
    '''Runs in parallel the specified command''')
    parser.add_argument('command', help='command to be runned')
    parser.add_argument('arguments', help='arguments for the command', nargs='*')

    args = parser.parse_args()

    execute_command_parallel(args.command, args.arguments)

def execute_command(command, arguments, filler):
    from subprocess import Popen, PIPE
    command = command.replace('{}', filler)
    arguments = [arg.replace('{}', filler) for arg in arguments]
    #FIXME: does it make any sense to pipe the stdout?
    f = Popen(" ".join([command] + arguments), stdout=PIPE, shell=True)
    for line in f.stdout:
        print line
    return f.wait()

def make_jobs(command, arguments, fillers):
    """
    creates a list of KybJob objects,
    which carry all information needed
    for a function to be executed on SGE:
    - function object
    - arguments
    - settings
    """

    # create empty job vector
    jobs=[]

    # create job objects
    for filler in fillers:
        job = KybJob(execute_command, [command, arguments, filler]) 
        job.h_vmem="2G"
        job.h_cpu='4:0:0'
        job.hosts='compute-0-1|compute-0-2|compute-0-3|compute-0-4|compute-0-5'
        
        jobs.append(job)
        

    return jobs




def execute_command_parallel(command, arguments, debug=False):
    """
    run a set of jobs on cluster
    """

    fillers = [l.strip() for l in sys.stdin]

    functionJobs = make_jobs(command, arguments, fillers)
    
    processedFunctionJobs = process_jobs(functionJobs,local=debug)

    for job in processedFunctionJobs:
        with open(job.log_stdout_fn) as f:
            for l in f:
                print l

if __name__ == "__main__":
    main()

