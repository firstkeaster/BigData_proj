import sys
import pyspark
from csv import reader
import string
import numpy as np
import csv
import argparse
import time
from pyspark.sql import SparkSession

splitter='\t'
divider=', '

fout=open('C_I_E_out.csv','w')

sc = pyspark.SparkContext()
spark=SparkSession(sc)
data=sys.argv[1]
table=spark.read.format('csv').options(header='true',inferschema='true').load(data)

list_include=sys.argv[2].split(',')
list_exclude=sys.argv[3].split(',')
list_all=list_include+list_exclude

cols=table.schema.names

col_count=len(cols)    

col_legal=[]

all_sigsig=0

for i in cols:
    sigsig=0
    for j in list_include:
        col_set=table.select(i).rdd.flatMap(lambda x:x).collect()
        if j not in col_set:
            sigsig=1
            break
    if sigsig==0:
        for j in list_exclude:
            col_set=table.select(i).rdd.flatMap(lambda x:x).collect()
            if j in col_set:
                sigsig=1
                break
    if sigsig==0:
        col_legal.append(i)
        all_sigsig=1
        print('legal col:',i)
        fout.write('legal col: '+str(i))

if all_sigsig==0:
    print('No column include the items!')
    fout.write('No column include the items!')

fout.close()