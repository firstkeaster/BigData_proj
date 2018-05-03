# !/usr/bin/env python

import sys
import string
import numpy as np
import csv
import argparse
import time

splitter='\t'
divider=', '

parser = argparse.ArgumentParser(description='SP_Char')

parser.add_argument('--filedir', type=list, default=[],
                    help='file directory')



args = parser.parse_args()
filedir=args.filedir
# filedir='2012_SAT_Results.csv'
filedir=''.join(filedir)

sp_char=set('",.<>/?:;[]{}\|-_=+!@#$%^&*()~`'+"'")

f=open(filedir)
fout=open('S_P_C_out.csv','w')

#record each column has which special char
dict_cal={}

line_count=0
col_count=0

line_z=f.readline()
line_z = line_z.strip()
columns = list(csv.reader([line_z],delimiter=','))[0]

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
    if col_count==0:
        col_count=len(entry)
        for i in range(col_count):
            dict_cal[i]=set()
    for i in range(col_count):
        if set(entry[i])&sp_char:
            dict_cal[i]=dict_cal[i].union(set(entry[i])&sp_char)
        
sigsig=0
for i in dict_cal:
    if len(dict_cal[i])>0:
        print('Column '+str(i)+' has sp char:',dict_cal[i])
        fout.write('Column '+str(i)+' has sp char: '+str(dict_cal[i]))
        sigsig=1
        
if sigsig==0:
    print('No column has sp char! ;)')
    fout.write('No column has sp char! ;)')

            
f.close()
fout.close()
