import sys
import pyspark
from csv import reader
sc = pyspark.SparkContext()
data=sys.argv[1]
table=spark.read.format('csv').options(header='true',inferschema='true').load(data) 
#table.createOrReplaceTempView("table") 

cols=[]
for x in sys.argv[1:]:
    cols.append(x)
inter=table.select(cols[0]).distinct()
for i in cols[1:]:
    col_new=table.select(i).distinct()
    inter=inter.intersection(col_new)

print(inter)
inter.write.csv('out.csv')