# !/usr/bin/env python

##Mapper

import sys
import string
import numpy as np
import csv
import argparse
import time

splitter='\t'
divider=', '

parser = argparse.ArgumentParser(description='Col_In_Ex')
parser.add_argument('--include', type=list, default=[],
                    help='items which some cols should include')
parser.add_argument('--exclude', type=list, default=[],
                    help='items which some cols should exclude')

args = parser.parse_args()

list_include=args.include
list_exclude=args.exclude
list_all=list_include+list_exclude

#Build a dictionary for all items, index them with the include and exclude list
dict_all={}
for i in list_all:
    dict_all[i]=[]


line_count=0
col_count=0

for line in sys.stdin:
    
    #Remove leading and trailing whitespace
    line_count+=1
    line = line.strip()
    entry = csv.reader([line],delimiter=',')
    entry=list(entry)[0]
    if col_count==0:
        col_count=len(entry)
    
    list_intsec=list(set(list_all).intersection(set(entry)))
    if list_intsec:
        #Append column number for all candidate items
        for i in list_intsec:
            dict_all[i].append(entry.index(i))
            
if list_include:
    for i in list_include:
        print(str(i)+divider+'1'+splitter,dict_all[i])
    if list_exclude:
        for i in list_exclude:
            print(str(i)+divider+'2'+splitter,dict_all[i])
else:
    print('No column include the items!')
