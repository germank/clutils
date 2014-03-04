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
logging.basicConfig(level=logging.INFO)
import argparse
import sys
import os
import yaml
from clutils.pipeline import CommandLineJob, Pipeline


def main():
    parser = argparse.ArgumentParser(description=
    '''Runs in parallel the specified command''')
    parser.add_argument('-n', '--name', default='gridparallel_logs')
    parser.add_argument('-c', '--config')
    parser.add_argument('-D', '--debug', action='store_true', default=False)
    parser.add_argument('command', help='command to be runned')
    parser.add_argument('arguments', help='arguments for the command', nargs='*')

    args = parser.parse_args()
    
    if args.config:
        config = yaml.load(file(args.config))
    else:
        config = {}
    
    execute_command_parallel(os.path.join(os.getcwd(),args.name), 
                             args.command, args.arguments, config, args.debug)

def execute_command(command, arguments, filler):
    from subprocess import Popen, PIPE
    command = command.replace('{}', filler)
    arguments = [arg.replace('{}', filler) for arg in arguments]
    #FIXME: does it make any sense to pipe the stdout?
    f = Popen(" ".join([command] + arguments), stdout=PIPE, shell=True)
    for line in f.stdout:
        print line.strip()
    return f.wait()

def make_jobs(work_path, command, arguments, fillers):
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
        command_filler = command.replace('{}', filler)
        arguments_filler = [arg.replace('{}', filler) for arg in arguments]
        modulename = filler
        job = CommandLineJob(work_path, modulename,
                             command_filler, arguments_filler) 
        jobs.append(job)
        

    return jobs




def execute_command_parallel(work_path, command, arguments, config, debug):
    """
    run a set of jobs on cluster
    """

    fillers = [l.strip() for l in sys.stdin]

    pl = Pipeline(work_path)    
    functionJobs = make_jobs(work_path, command, arguments, fillers)
    pl.add_stage(*functionJobs)
    pl.run(debug, False, config)

#    for job in processedFunctionJobs:
#        with open(job.log_stdout_fn) as f:
#            for l in f:
#                print l

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print 'Aborted!'

