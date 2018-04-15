# !/usr/bin/env python

##Reducer

import sys
import string
import numpy
import csv
import argparse
import time

splitter='\t'
divider=', '

list_include=[]
list_exclude=[]

dict_in={}
dict_ex={}

col_selected=[]

for line in sys.stdin:
    if line=='No column include the items!':
        print('No column include the items!')
        break
    line=line.strip()
    entry=line.split(splitter)
    
    key=entry[0].strip().split(divider)
    value=entry[1].strip().split(divider)
    
    if key[1]==1:
        list_include.append(key[0])
        dict_in[key[0]]=value
    else:
        list_exclude.append(key[0])
        dict_ex[key[0]]=value
        
if list_include:
    for i in dict_in[list_include[0]]:
        sigsig=0
        for j in list_include[1:]:
            if i not in dict_in[j]:
                sigsig=1
                break
        for k in list_exclude:
            if i in dict_ex[k]:
                sigsig=1
                break
        if sigsig==1:
            print('column:',i)      