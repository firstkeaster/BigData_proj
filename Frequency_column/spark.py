import sys
import pyspark
from csv import reader
sc = pyspark.SparkContext()
data=sys.argv[1]
table=spark.read.format('csv').options(header='true',inferschema='true').load(data) 
table.createOrReplaceTempView("table") 

cols=[]
for x in sys.argv[1:]:
    cols.append(x)
x_old=None
for i in cols:
    x_new=table.select(i).distinct()
    if x_old!=None:
        x_old=x_old.union(x_new)
    else:
        x_old=x_new
            
out=x_old.groupby(cols[0]).count().sort('count') 
print(out.take(5))
out.write.csv('out.csv')
