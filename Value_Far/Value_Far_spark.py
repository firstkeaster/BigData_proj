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

fout=open('V_F_out.csv','w')

sc = pyspark.SparkContext()
spark=SparkSession(sc)
data=sys.argv[1]
table=spark.read.format('csv').options(header='true',inferschema='true').load(data)
column=int(sys.argv[2])
range_var=float(sys.argv[3])

cols=table.schema.names

tp_len_data=table.select(cols[column]).rdd.flatMap(lambda x:x).collect()
len_data=[]
for i in tp_len_data:
    try:
        len_data.append(float(i))
    except:
        continue

stderr=np.std(len_data)
mean=np.mean(len_data)
thre_upper=mean+range_var*stderr
thre_lower=mean-range_var*stderr

print('Mean:',mean)
fout.write('Mean: '+str(mean))
print('Stderr:',stderr)
fout.write('Stderr: '+str(stderr))

sigsig=0

for j in range(len(len_data)):
    if len_data[j]>=thre_upper or len_data[j]<=thre_lower:
        print('row:',j)
        fout.write(str('row: '+str(j)))
        sigsig=1
        
if sigsig==0:
    print('No over-length rows!')
    fout.write(str('No over-lenght rows!'))

fout.close()