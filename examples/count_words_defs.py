from clutils.pipeline import JobModule
from clutils.pins import TextFilePin, DictionaryPin, PinMultiplex
from collections import Counter

class CountWords(JobModule):
    def setup(self):
        #Register the pins that the count module will use to communicate
        self.register_pins(
        #It reads from a text file
                           TextFilePin("input"),
        #It outputs into a dictionary (A ScalarPin would have done just as fine)
                           DictionaryPin("output", Counter))
    
    def run(self, filename, zipped):
        #We need to open the TextFilePin
        self['input'].open(filename, zipped)
        #We can read from it as if it were just any file
        for l in self['input'].read():
            if not l.startswith('<'):
                word = l.rstrip().split('\t')[0]
                #Counts are directly stored into the output pin 
                #(It will get after exiting the procedure)
                self['output'][word] += 1
            
    

class SumCounts(JobModule):
    def setup(self):
        #Register the pins that the sum module will use to communicate
        self.register_pins(
        #Here's the magic: we input data from the output of the previous module
        #using the PinMultiplex
                           PinMultiplex("input"),
        #We output the final values into a dictionary pin 
                           DictionaryPin("output", Counter))
    
    def run(self):
        #Reading the multiplex involves reading each of the connected pins
        for partial_count in self['input'].read():
            #The partial_count is the the read dictionary
            for word, c in partial_count.iteritems():
                #Count each of the partial counts into a global count
                self['output'][word] += c