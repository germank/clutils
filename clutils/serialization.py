import pickle
import os

class ClBaseSerializer(object):
    def __init__(self, filename):
        self.filename = filename
    def file_exists(self):
        return os.path.isfile(self.filename)

class PklSerializer(ClBaseSerializer):
    def __init__(self, filename):
        super(PklSerializer, self).__init__(filename + '.pkl')
    def save_dict(self, d):
        pickle.dump(d, file(self.filename, 'w'))
    def save_scalar(self, s):
        pickle.dump(s, file(self.filename, 'w'))
    def read(self):
        return pickle.load(file(self.filename))

class TxtSerializer(ClBaseSerializer):
    def __init__(self, filename):
        super(TxtSerializer, self).__init__(filename + '.txt')
    def save_dict(self, d):
        with open(self.filename, 'w') as f:
            for k,v in sorted(d.iteritems()):
                f.write('{0}\t{1}\n'.format(k,v))
    def save_scalar(self, s):
        with open(self.filename, 'w') as f:
            f.write("{0}".format(s))
    def read(self):
        return dict(l.split('\t') for l in file(self.output_path))
        