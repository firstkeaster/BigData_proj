# !/usr/bin/env python

import sys
import string
import numpy
import csv
import argparse
import time

parser = argparse.ArgumentParser(description='BigData_Length_Query')
parser.add_argument('--column', type=list, default=[],
                    help='items which some cols should exclude')

args = parser.parse_args()
column=args.column


line_count=0
sum_var=0
sum_valu=0
col=[]
i=0

for line in sys.stdin:
    
    #Remove leading and trailing whitespace
    line_count+=1
    line = line.strip()
    entry = csv.reader([line],delimiter=',')
    entry=list(entry)[0]
    col.append(len(str(entry[column])))
    sum_var+=col[i]**2
    sum_valu+=col[i]
    i+=1
    
line_count=float(i)
print(line_count,sum_var,sum_valu)
var=(sum_var-(sum_valu**2)/line_count)/line_count
print(var)
mean=sum_valu/line_count
thre_upper=mean+0.2*var
thre_lower=mean-0.2*var

for j in range(len(col)):
    if col[j]>=thre_upper or col[j]<=thre_lower:
        print(j)
