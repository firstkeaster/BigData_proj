# !/usr/bin/env python

import sys
import string
import numpy
import csv
import argparse
import time

line_count=0
col_count=0

dic = {}

#for line in a:
for line in sys.stdin:
    
    #Remove leading and trailing whitespace
    line_count+=1
    line = line.strip()
    entry = csv.reader([line],delimiter=',')
    entry=list(entry)[0]
    #entry=line
    if col_count==0:
        col_count=len(entry)
        for i in range(col_count):
            dic[i]={}
    
    for i in range(col_count):
        if entry[i] in dic[i].keys():
            dic[i][entry[i]]+=1
        else:
            dic[i][entry[i]]=1
    

for i in range(col_count):
    if len(dic[i].keys())==line_count:
        print('column:',i)