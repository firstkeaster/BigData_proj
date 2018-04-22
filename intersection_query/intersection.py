import sys
from operator import add
from pyspark import SparkContext

sc = SparkContext()

tableA = sc.textFile(sys.argv[1], 1)..mapPartitions(lambda x: reader(x))
tableB = sc.textFile(sys.argv[2], 1)..mapPartitions(lambda x: reader(x))
tableA = tableA.filter(lambda x: x.distinct().count()>=0.9*len(x))
tableB = tableB.filter(lambda x: x.distinct().count()>=0.9*len(x))
result = tableA.intersection(tableB).collect()

result.saveAsTextFile('intersection_query.out')


