import fileinput
import logging
import os
from collections import MutableMapping
from clutils.aux import mkdir_p
from clutils.serialization import PklSerializer


def gziplines(fname):
    from subprocess import Popen, PIPE
    f = Popen(['zcat' ] + [fname], stdout=PIPE)
    for line in f.stdout:
        yield line
        

class Pin(object):
    def __init__(self, name):
        super(Pin, self).__init__()
        self.name = name
        self.source = None

    def initialize(self, work_path):
        pass
    
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
    def __init__(self, name):
        super(OutputPin, self).__init__(name)
        
    def initialize(self, work_path):
        self.output_path = os.path.join(work_path, self.name)
        mkdir_p(self.output_path)
        
class PinMultiplex(Pin):
    '''
    Allows to connect many pins to itself
    '''
    def __init__(self, name):
        super(PinMultiplex, self).__init__(name)
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
            yield pin.read()
    
    def file_exists(self):
        for pin in self.pins:
            if not pin.file_exists():
                return False
        return True
                
class ScalarPin(OutputPin):
    def __init__(self, name, serializer_type=PklSerializer):
        super(ScalarPin, self).__init__(name)
        self.serializer_type = serializer_type

    def initialize(self, work_path):
        super(ScalarPin, self).initialize(work_path)
        self.serializer = self.serializer_type(os.path.join(self.output_path,
            '{0}.pkl'.format(self.name)))

    def write(self, data):
        self.serializer.save_scalar(data)

    def read(self):
        return self.serializer.read()

    def file_exists(self):
        return self.serializer.file_exists()
        
class DictionaryPin(OutputPin, MutableMapping):
    def __init__(self, name, dict_class=dict, serializer_type=PklSerializer):
        super(DictionaryPin, self).__init__(name)
        self.serializer_type = serializer_type
        self.dict_class = dict_class
    
    def initialize(self, work_path):
        super(DictionaryPin, self).initialize(work_path)
        self.serializer = self.serializer_type(os.path.join(self.output_path,
            '{0}'.format(self.name)))
        self.data = self.dict_class()

    def write(self, data):
        self.data = data
    
    def __getitem__(self, k):
        return self.data.__getitem__(k)

    def __setitem__(self, k, v):
        self.data.__setitem__(k, v)
    
    def __delitem__(self, key):
        self.data.__delitem__(key)
    
    def __len__(self):
        return self.data.__len__()
    
    def __iter__(self):
        return self.data.__iter__()
        
    def close(self):
        self.serializer.save_dict(self.data)
        
    def read(self):
        self.data = self.serializer.read()
        return self.data

    def file_exists(self):
        return self.serializer.file_exists()


class TextFilePin(Pin):
    def __init__(self, name, ):
        super(TextFilePin, self).__init__(name)
        self.closed = True
        
    
    def open(self, filename, gzip=False):
        '''Opens the pin for reading'''
        self.filename = filename
        self.gzip = gzip
        self.closed = False

    def read(self):
        if self.closed:
            raise RuntimeError("Cannot read closed TextFilePin. Please, call 'open' first")
        if self.gzip:
            return gziplines(self.filename)
        else:
            return fileinput.FileInput(self.filename)
        
    def close(self):
        try:
            del(self.filename)
            del(self.gzip)
        except AttributeError:
            pass
        self.closed=True
    
    def file_exists(self):
        #This is for output pins, so we don't really care here
        return True

