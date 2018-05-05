import sys
import pyspark
from csv import reader
from pyspark.mllib.stat import Statistics

sc = pyspark.SparkContext()
data=sys.argv[1]
parking = sc.textFile(data, 1)
parking = parking.mapPartitions(lambda x: reader(x))
header = parking.first()
parking=parking.filter(lambda row:row != header)
l=[]
for i in range(len(parking.take(1)[0])):
    try:
        state = parking.map(lambda x: [float(x[i])])
        summary = Statistics.colStats(state)
        count=summary.count()
        max=summary.max()[0]
        mean=summary.mean()[0]
        min=summary.min()[0]
        nonzero=summary.numNonzeros()[0]
        var=summary.variance()[0]
        l.append([i,count,max,mean,min,nonzero,var])
    except:
        continue

result=sc.parallelize(l)

out = result.map(lambda x: str(x[0]) + '\tcount:' + str(x[1])+' max:' + str(x[2])+' mean:' + str(x[3])+' min:' + str(x[4])+' nonzerocount:' + str(x[5])+' var:' + str(x[6]))
out.saveAsTextFile("task_stat.out")
sc.stop()