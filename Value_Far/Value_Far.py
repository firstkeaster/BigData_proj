# !/usr/bin/env python

import sys
import string
import numpy
import csv
import argparse
import time
import math

parser = argparse.ArgumentParser(description='BigData_Length_Query')
parser.add_argument('--column', type=list, default=[],
                    help='items which some cols should exclude')

parser.add_argument('--filedir', type=list, default=[],
                    help='file directory')

parser.add_argument('--range', type=list, default=[],
                    help='range*var')

args = parser.parse_args()

filedir=args.filedir
filedir=''.join(filedir)
column=args.column
column=int(''.join(column))
range_var=args.range
range_var=''.join(range_var)


line_count=0
sum_var=0
sum_valu=0
col=[]
i=0

f=open(filedir)
fout=open('V_F_out.csv','w')

#Throught the first line
line_tp=f.readline()

while True:
#for line in sys.stdin:
    line=f.readline()
    if not line:
        break
    #Remove leading and trailing whitespace
    line_count+=1
    line = line.strip()
    entry = csv.reader([line],delimiter=',')
    entry=list(entry)[0]
    try:
        col.append(float(entry[column]))
        sum_var+=col[i]**2
        sum_valu+=col[i]
        i+=1
    except:
        continue
    
line_count=float(i)
#print(line_count,sum_var,sum_valu)
var=(sum_var-(sum_valu**2)/line_count)/line_count
#print(var)
mean=sum_valu/line_count
stderr=math.sqrt(var)
thre_upper=mean+float(range_var)*stderr
thre_lower=mean-float(range_var)*stderr

print('Mean:',mean)
fout.write('Mean: '+str(mean))
print('Stderr:',stderr)
fout.write('Stderr: '+str(stderr))

sigsig=0

for j in range(len(col)):
    if col[j]>=thre_upper or col[j]<=thre_lower:
        print('row:',j)
        fout.write(str('row: '+str(j)))
        sigsig=1

if sigsig==0:
    print('No outlier rows!')
    fout.write(str('No outlier rows!'))

f.close()
fout.close()