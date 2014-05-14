from clutils.pipeline import JobModule, Pipeline
from clutils.pins import ScalarPin, PinMultiplex


class SimpleJob(JobModule):
    def setup(self):
        self.register_pins(ScalarPin("output"))
        
    def run(self, f, args):
        rv = f(*args)
        self['output'].write(rv)

class MergeJob(JobModule):
    def setup(self):
        self.register_pins(PinMultiplex("input"), ScalarPin("output"))
    
    def run(self, f, args):
        rv = f(*args + list(self['input'].read()))
        self['output'].write(rv)

class CommandLineJob(JobModule):
    def run(self, command, arguments):
        from subprocess import Popen, PIPE
        f = Popen(" ".join([command] + arguments), stdout=PIPE, shell=True)
        for line in f.stdout:
            print line.strip()
        return f.wait()
    
def create_parallel_pipeline(work_path, f, args_list):
    '''Run a function f with many arguments in parallel'''
    p = Pipeline(work_path)
    p.register_pins(PinMultiplex("output"))
    for i, args in enumerate(args_list):
        job = SimpleJob(str(i))
        job.set_args(f, args)
        job['output'].connect_to(p['output'])
        p.add_module(job)
    return p


def create_map_reduce_pipeline(work_path, m, r, args_list):
    '''Run a function f with many arguments in parallel'''
    p = Pipeline(work_path)
    reduce_job = MergeJob("reduce")
    reduce_job.set_args(r, [])
    reduce_job['output'].connect_to(p['output'])
    for i, args in enumerate(args_list):
        map_job = SimpleJob("map", str(i))
        map_job.set_args(m, args)
        map_job['output'].connect_to(reduce_job['input'])
        p.add_module(map_job)
    p.add_stage()
    p.add_module(reduce_job)
    return p

