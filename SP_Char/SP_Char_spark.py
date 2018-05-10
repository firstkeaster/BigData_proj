import sys
import pyspark
from csv import reader
import string
import numpy as np
import csv
import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import size

splitter='\t'
divider=', '

fout=open('S_P_C_out.csv','w')
sp_char=set('",.<>/?:;[]{}\|-_=+!@#$%^&*()~`'+"'")

sc = pyspark.SparkContext()
spark=SparkSession(sc)
data=sys.argv[1]
table=spark.read.format('csv').options(header='true',inferschema='true').load(data)

cols=table.schema.names

dict_cal={}

for i in cols:
    list_cal=table.select(i).rdd.flatMap(lambda x:x).collect()
    set_sp=set()
    for j in list_cal:
        set_sp=set_sp.union(set(j)&sp_char)
    dict_cal[i]=set_sp

sigsig=0
for i in dict_cal:
    if len(dict_cal[i])>0:
        print('Column: '+str(i)+' has sp char:',dict_cal[i])
        fout.write('Column: '+str(i)+' has sp char: '+str(dict_cal[i]))
        sigsig=1
        
if sigsig==0:
    print('No column has sp char! ;)')
    fout.write('No column has sp char! ;)')

fout.close()