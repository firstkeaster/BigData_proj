import sys
import pyspark
from csv import reader
sc = pyspark.SparkContext()
#parking = sc.textFile(sys.argv[1], 1)
#parking = parking.mapPartitions(lambda x: reader(x))
parking=spark.read.format('csv').options(header='true',inferschema='true').load('/user/ecc290/HW1data/parking-violations-header.csv') 
parking.createOrReplaceTempView("parking") 

cols=[]
for x in sys.argv:
    cols.append(x)
x_old=None
#cols=['issuer_code','meter_number','plate_type','registration_state']
for i in cols:
    x_new=parking.select(i).distinct()
    if x_old!=None:
        x_old=x_old.union(x_new)
    else:
        x_old=x_new
            
b=x_old.groupby(cols[0]).count().sort('count') 
print(b.take(5))
b.write.csv('b.csv')