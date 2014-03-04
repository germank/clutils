from pythongrid import KybJob, process_jobs
import os
import fnmatch
import logging
from aux import mkdir_p, dict_merge, dict_flatten
import contextlib
import time
from clutils.pins import PinMultiplex
from abc import abstractmethod


class BaseModule(object):
    def __init__(self, pins = None):
        if not pins:
            pins = {}
        self.pins = pins
        try:
            self.setup()
        except AttributeError, e:
            if 'setup' in e.message:
                raise RuntimeError("You must define a setup method to initalize the "
                               "pins in class {0}".format(self.__class__))
            else:
                raise
    
    @abstractmethod
    def setup(self):
        """Override this method to register the module Pins"""
        pass
            
        
    def __getitem__(self, pin_name):
        return self.pins[pin_name]
    
    def register_pins(self, *pins):
        for pin in pins:
            self.pins[pin.get_name()] = pin
    
    def close_pins(self, *args):
        for pin in self.pins.itervalues():
            pin.close()
    
    def initialize(self, work_path):
        for pin in self.pins.itervalues():
            pin.initialize(work_path)
            

class JobModule(BaseModule):
    def __init__(self, *name):
        '''
        Args:
            name: A string or a list of strings that will be joined with the 
                parent_work_path to create the working path of the job
            args: Every other argument is directly passed to the run function
                WARNING: every argument should be serializable
        '''
        
        self.args = []
        self.name = name
        super(JobModule, self).__init__()

    def initialize(self, work_path):
        '''
            work_path: A path in which to create the relevant directories
                for this module.
        '''
        if isinstance(self.name, basestring):
            self.work_path = os.path.join(work_path, self.name)
        else:
            #assume the name is a iterable list of names
            self.work_path = os.path.join(work_path, os.path.join(*self.name))
        mkdir_p(self.work_path)
        super(JobModule, self).initialize(self.work_path)
        logging.info("Job Initialized at {0}".format(self.work_path))
                
    def set_args(self, *args):
        self.args = args
    
    @abstractmethod
    def run(self, *args, **kwargs):
        """Override to define the code that will be executed by the job"""
        pass
    
    def __str__(self):
        return self.work_path
    
    def __repr__(self):
        return "{0}({1})".format(type(self).__name__,self.work_path)

    def get_work_path(self):
        return self.work_path

    def finished(self):
        '''
        If the pipeline is run with the resume option, this function is called
        to ask if the module needs to be runned. If it returns True, then the
        module won't be run again
        
        Override to specify different semantics 
        '''
        for pin in self.pins.itervalues():
            if not pin.file_exists():
                return False
        return True

    def finalize(self):
        for pin in self.pins.itervalues():
            pin.finalize()


class Pipeline(BaseModule):
    def __init__(self, work_path):
        mkdir_p(work_path)
        self.stages = []
        self.add_stage()
        self.ctx_mgrs = []
        self.work_path = work_path
        super(Pipeline, self).__init__()
    
    def setup(self):
        self.register_pins(PinMultiplex("output"))
        
    def add_stage(self, *modules):
        '''creates a stage, which is a set of modules
        executed concurrently. Stages, on the other hand, 
         are executed consecutively'''
        self.stages.append(list(modules))
    
    def add_module(self, module):
        '''adds a module to the current stage'''
        self.stages[-1].append(module)

    def add_context_mgr(self, ctx_mgr):
        self.ctx_mgrs.append(ctx_mgr)
        
    
    def apply_config(self, config, module, job):
        #flat all keys into tuples
        flattened_config = dict_flatten(config)
        #the final keys in the config represent attributes of the modules
        #so we un-flatten them
        unflat_config = {}
        for k,v in flattened_config.iteritems():
            #transform the key into a path
            nk = os.path.join(self.work_path,"/".join(k[:-1])) + '*'
            if nk not in unflat_config:
                unflat_config[nk] = {}
            unflat_config[nk][k[-1]] = v
        #apply from more general to more specific
        config_order = sorted(unflat_config.keys(), cmp=lambda a,b: 
            1 if fnmatch.fnmatch(a,b) else -1 if fnmatch.fnmatch(b,a) else 0)
        #apply configuration
        for config_k in config_order:
            if fnmatch.fnmatch(module.work_path, config_k):
                logging.debug('Applying config {0} to job {1}'.format(
                            unflat_config[config_k], module))
                for k,v in unflat_config[config_k].iteritems():
                    #can erase default configuration settings (not recommended)
                    if v is not None:
                        setattr(job, k, v)

    def run(self, debug=False, resume=False, config=None, pythonpathdir=None):
        #Initialize modules
        for stage in self.stages:
                for module in stage:
                    module.initialize(self.work_path)
        logging.debug("Initialization Finished")
        #default configuration
        default_config = {'*': {'h_cpu': '1:0:0', 'h_vmem': '1G'}}
        if not config:
            logging.debug("Using default configuration")
            config = default_config
        else:
            if not isinstance(config, dict):
                raise TypeError('config is given as a dictionary')
            if all(isinstance(v, basestring) for v in config.values()):
                logging.debug("Using same configuration for all modules")
                config = {'*': config}
            config = dict_merge(config, default_config)
            logging.debug("Merged user-provided configuration: {0}".format(config))
        #By default, if no path is given, it adds to the python path
        #the directory where the module was defined
        if not pythonpathdir:
            import inspect
            pythonpathdirs = set()
            #Add to the pythonpath the directory where the pipeline is defined  
            pythonpathdirs.add(os.path.dirname(os.path.abspath(
                                                inspect.getfile(type(self)))))
            for stage in self.stages:
                for module in stage:
                    #Add to the pythonpath the directory where each module
                    #is defined
                    pythonpathdirs.add(os.path.dirname(os.path.abspath(
                                                inspect.getfile(type(module)))))
                    for arg in module.args:
                        try:
                            #Add to the pythonpath the directory where each argument
                            #is defined
                            pythonpathdirs.add(os.path.dirname(
                                        os.path.abspath(inspect.getfile(arg))))
                        except TypeError:
                            pass
            pythonpathdir = ":".join(pythonpathdirs)
            logging.debug("Adding potential useful paths to the pythonpath: {0}"
                          .format(pythonpathdir))

        with contextlib.nested(*self.ctx_mgrs):
            if debug:
                #give time for context managers to initialize
                #(for kyototycoon debugging)
                time.sleep(1)
            for stage in self.stages:
                jobs = []
                for module in stage:
                    if not resume or not module.finished():
                        job = KybJob(run_clmodule, [module],
                            pythonpathdir=pythonpathdir,
                            logdir=os.path.abspath(module.work_path))
                        self.apply_config(config, module, job)
                        jobs.append(job)
                    elif resume:
                        logging.info("{0}: SKIP".format(module))
                process_jobs(jobs, local=debug)
                for module in stage:
                    module.finalize()
                    

def run_clmodule(module):
    logging.info("{0}: running".format(module))
    try:
        module.run(*module.args)
    except AttributeError, e:
        if 'run' in e.message:
            raise RuntimeError("You must define the run method in the JobModule "
                           "instance.")
        else: raise
    logging.info("{0}: closing pins".format(module))
    module.close_pins()
    logging.info("{0}: finalizing".format(module))
    module.finalize()
    return True
