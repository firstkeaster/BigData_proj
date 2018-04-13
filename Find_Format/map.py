#!/usr/bin/env python
import sys
import string
from csv import reader

colindex= int(sys.argv[1]) 
colname= sys.argv[2]

change=False
for line in sys.stdin:    
    csv_reader = reader([line])
    li = None
    for row in csv_reader:
        li = row
        code=li[colindex]
        s=list(code)
        for char in s:
            print ('{0}\t{1}'.format(colname,char))
        

    