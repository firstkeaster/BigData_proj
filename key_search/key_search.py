import sys
import pyspark
from csv import reader
sc = pyspark.SparkContext()
data1=sys.argv[1]
data2=sys.argv[2]

table1=spark.read.format('csv').options(header='true',inferschema='true').load(data1) 
#table1.createOrReplaceTempView("table1") 

table2=spark.read.format('csv').options(header='true',inferschema='true').load(data2) 
#table2.createOrReplaceTempView("table2")

col = {}
cols1= table1.first()
cols2= table2.first()

for i in cols1:
    c1 = table1.select(i).distinct()
    for j in cols2:
        key = i +', '+ j
        c2 = table2.select(i).distinct()
        inter = c1.intersection(c2)
        uni = c1.union(c2)
        jac = len(inter)/len(uni)
        col[key] = jac
    
result = sorted(col.items(), key = lambda x: x[1], reverse = True)[5]
    
result.write.csv('out.csv')