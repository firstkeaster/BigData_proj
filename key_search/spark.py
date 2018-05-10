
import sys
import pyspark
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from pyspark.context import SparkContext

sc = SparkContext('local')
spark = SparkSession(sc)

df1 = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
df2 = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])


out=open('out.csv','w')

rows1 = df1.count()
rows2 = df2.count()



key1 = [df1.columns[[df1.select(i).distinct().count() for i in df1.columns]==rows1]]
key2 = [df2.columns[[df2.select(i).distinct().count() for i in df2.columns]==rows2]]

def jaccard_similarity(x,y):
	intersection = (x.distinct().intersect(y.distinct())).count()
	union = (x.union(y)).distinct().count()
	return intersection/float(union)


sim = {}
for i in key1:
	for j in key2:
		sim[i+', '+j] = jaccard_similarity(df1.select(i),df2.select(j))

#print(sim)
result = sorted(sim.items(), key = lambda x: x[1], reverse = True)
print('join candidate columns and similarity are: %s'%result)


out.write('join candidate columns and similarity are: %s'%result)


out.close()

