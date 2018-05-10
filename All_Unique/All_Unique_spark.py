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

fout=open('A_U_out.csv','w')

sc = pyspark.SparkContext()
spark=SparkSession(sc)
data=sys.argv[1]
table=spark.read.format('csv').options(header='true',inferschema='true').load(data)

cols=table.schema.names

len_all=0
sigsig=0
for i in cols:
    if len_all==0:
        len_all=len(table.select(i).rdd.flatMap(lambda x:x).collect())
    len_dist=len(table.select(i).distinct().rdd.flatMap(lambda x:x).collect())
    if len_all==len_dist:
        sigsig=1
        print('All Unique col:',i)
        fout.write('All Unique col: '+str(i))
    
if sigsig==0:
    print('No column is all unique!')
    fout.write('No column is all unique!')
    
fout.close()