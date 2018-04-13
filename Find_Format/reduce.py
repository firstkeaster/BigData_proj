#!/usr/bin/env python
import sys
import string
import numpy

l=[]
current=None
for line in sys.stdin:
    key, value = line.split('\t')

    if current!=None:
        if key !=current:
            l=set(l)
            if len([x for x in l if x.isdigit()])/len(l)>0.95:
                print('{}\t{}'.format(current,'numerical'))
            else:
                print('{}\t{}'.format(current,'string'))
            l=[]
    l.append(value)
    current=key
    
l=set(l)
if len([x for x in l if x.isdigit()])/len(l)>0.95:
    print('{}\t{}'.format(current,'numerical'))
else:
    print('{}\t{}'.format(current,'string'))
  
    
