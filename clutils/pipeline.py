from pythongrid import KybJob, process_jobs
import os
import cPickle as pickle
import fileinput
import errno
import anydbm
import shelve
import fnmatch
import logging
logging.basicConfig(level=logging.INFO)
import contextlib
import time

#auxiliary functions
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def gziplines(fname):
    from subprocess import Popen, PIPE
    f = Popen(['zcat' ] + [fname], stdout=PIPE)
    for line in f.stdout:
        yield line

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
import collections

from collections import *
from itertools import *

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


class Pin(object):
    def __init__(self, module, name):
        super(Pin, self).__init__()
        self.name = name
        self.module = module
        self.source = None

    def get_name(self):
        return self.name

    def put_source(self, ot):
        self.source = ot
    
    def connect_to(self, ot):
        ot.put_source(self)

    def read(self):
        if self.source:
            return self.source.read()

    def dispose(self):
        '''Destroy the intermediate values of the pin'''
        #FIXME: some reference counting mechanism might be required
        pass
    
    def open(self):
        pass

    def close(self):
        pass

    def finalize(self):
        pass

class OutputPin(Pin):
    def __init__(self, module, name):
        super(OutputPin, self).__init__(module, name)
        self.output_path = os.path.join(module.get_work_path(), name)
        mkdir_p(self.output_path)

class ValuePin(OutputPin):
    def __init__(self, module, name):
        super(ValuePin, self).__init__(module, name)
        self.output_filename = os.path.join(self.output_path,
            '{0}.pkl'.format(self.name))

    def write(self, data):
        pickle.dump(data, file(self.output_filename, 'w'))

    def read(self):
        return pickle.load(file(self.output_filename))

    def file_exists(self):
        return os.path.isfile(self.output_filename)
    
class DictionaryPin(OutputPin):
    def __init__(self, module, name, storage='txt'):
        super(DictionaryPin, self).__init__(module, name)
        self.storage = storage
        if self.storage == 'txt':
            self.output_filename = os.path.join(self.output_path,
            '{0}.txt'.format(self.name))

    def write(self, data):
        if self.storage =='txt':
            with open(self.output_filename, 'w') as f:
                for k,v in data.iteritems():
                    f.write('{0}\t{1}\n'.format(k,v))

    def read(self):
        if self.storage == 'txt':
            return dict(l.split('\t') for l in file(self.output_path))

    def file_exists(self):
        return os.path.isfile(self.output_filename)

class ProxyPin(OutputPin):
    def __init__(self, module, name, real_subject):
        super(ProxyPin, self).__init__(module, name)
        self.real_subject = real_subject

    def __getattr__(self, k):
        if k == '__setstate__':
            return getattr(super(OutputPin, self), k)
        return getattr(self.real_subject, k)

    def open(self):
        self.real_subject.open()

    def close(self):
        self.real_subject.close()

    def __len__(self):
        return len(self.real_subject)


class ShelvePin(OutputPin):
    def __init__(self, module, name):
        super(ShelvePin, self).__init__(module, name)
        self.output = None
        self.output_filename = os.path.join(self.output_path,
            '{0}').format(self.name)
    
    def lazy_init(self):
        if self.output is None:
            self.output = shelve.open(self.output_filename)
    
    def __getitem__(self, k):
        self.lazy_init()
        return self.output[k]
        
    def __setitem__(self, k, v):
        self.lazy_init()
        self.output[k] = v

    def write(self, data_dict):
        self.lazy_init()
        self.output.update(data_dict)

    def read(self):
        self.lazy_init()
        return self.output

class DBMPin(OutputPin):
    def __init__(self, module, name, value_type=None):
        super(DBMPin, self).__init__(module, name)
        self.value_type = value_type
        self.output = None
        self.output_filename = os.path.join(self.output_path,
            '{0}').format(self.name)

    def lazy_init(self):
        if not self.output:
            self.output = anydbm.open(self.output_filename, 'c')
    
    def __getitem__(self, k):
        self.lazy_init()
        if self.value_type:
            return self.value_type(self.output[k])
        else:
            return self.output[k]
        
    def __setitem__(self, k, v):
        self.lazy_init()
        if self.value_type:
            self.output[k] = str(v)
        else:
            self.output[k] = v

    def write(self, data_dict):
        self.lazy_init()
        self.output.update(data_dict)

    def read(self):
        self.lazy_init()
        return self.output

class ListPin(Pin):
    def __init__(self, module, name):
        super(ListPin, self).__init__(module, name)
        self.pins=[]

    def __iter__(self):
        #FIXME: iterate over pins and let the user read it herself!
        #that way she can dispose unused pins afterwards
        return iter(self.read())

    def put_source(self, pin):
        self.append(pin)

    def append(self, pin):
        self.pins.append(pin)

    def read(self):
        for pin in self.pins:
            try: 
                yield pin.read()
            except KeyboardInterrupt:
                raise
            except:
                logging.exception("Error while reading pin "
                    "{0}".format(pin.get_name()))

        

class TextFilePin(Pin):
    def __init__(self, module, name, filename, gzip=False):
        super(TextFilePin, self).__init__(module, name)
        self.filename = filename
        self.gzip = gzip

    def read(self):
        if self.gzip:
            return gziplines(self.filename)
        else:
            return fileinput.FileInput(self.filename)

        

class JobModule(object):
    def __init__(self, work_path, pins = None):
        super(JobModule, self).__init__()
        self.work_path = work_path
        mkdir_p(self.work_path)
        if not pins:
            pins = {}
        self.pins = pins

    def __str__(self):
        return self.work_path
    
    def __repr__(self):
        return "{0}({1})".format(type(self).__name__,self.work_path)

    def __getitem__(self, pin_name):
        return self.pins[pin_name]

    def get_work_path(self):
        return self.work_path

    def setup(self, *pins):
        for pin in pins:
            self.pins[pin.get_name()] = pin

    def open_pins(self):
        for pin in self.pins.itervalues():
            pin.open()

    def close_pins(self, *args):
        for pin in self.pins.itervalues():
            pin.close()

    def run(self):
        '''
        Main loop of the job. To be overriden on inheriting classes
        '''
        pass

    def finished(self):
        '''
        If the pipeline is run with the resume option, this function is called
        to ask if the module needs to be runned. If it returns True, then the
        module won't be run again
        '''
        pass

    def finalize(self):
        for pin in self.pins.itervalues():
            pin.finalize()

class CommandLineJob(JobModule):
    def __init__(self, work_path, command, arguments):
        super(CommandLineJob, self).__init__(work_path)
        self.command = command
        self.arguments = arguments
    
    def run(self):
        from subprocess import Popen, PIPE
        f = Popen(" ".join([self.command] + self.arguments), stdout=PIPE, shell=True)
        for line in f.stdout:
            print line.strip()
        return f.wait()

class Pipeline(object):
    def __init__(self, work_path):
        super(Pipeline, self).__init__()
        mkdir_p(work_path)
        self.stages = []
        self.ctx_mgrs = []
        self.work_path = work_path

    def add_stage(self, *modules):
        logging.info("{0} modules loaded".format(len(modules)))
        self.stages.append(modules)

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
        #default configuration
        default_config = {'*': {'h_cpu': '24:0:0', 'h_vmem': '7900M'}}
        if not config:
            config = default_config
        else:
            if not isinstance(config, dict):
                raise TypeError('config is given as a dictionary')
            if all(isinstance(v, basestring) for v in config.values()):
                config = {'*': config}
            config = dict_merge(config, default_config)
        if not pythonpathdir:
            import inspect
            pythonpathdir = os.path.dirname(os.path.abspath(inspect.getfile(type(self))))
            
        with contextlib.nested(*self.ctx_mgrs):
            if debug:
                #give time for context managers to initialize
                #(for kyototycoon debugging)
                time.sleep(1)
            for i_stage, stage in enumerate(self.stages):
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
    logging.info("{0}: openinig pins".format(module))
    module.open_pins()
    logging.info("{0}: running".format(module))
    module.run()
    logging.info("{0}: closing pins".format(module))
    module.close_pins()
    logging.info("{0}: finalizing".format(module))
    module.finalize()
    return True
