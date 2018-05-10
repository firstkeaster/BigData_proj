import sys
import pyspark
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format
from pyspark.context import SparkContext

sc = SparkContext('local')
spark = SparkSession(sc)

df1=spark.read.format('csv').options(header='True',inferschema='true').load(sys.argv[1]) 
#df1.createOrReplaceTempView("table") 

out = open('inter_out.out','w')

#print('columns:',len(df1.dtypes))
#print('rows:',df1.count())

cols = list(map(int, sys.argv[2].split(',')))
#cols = list(map(int, sys.argv[2:]))
col_name = [df1.columns[i] for i in cols]

print('column name: '+', '.join([str(i) for i in col_name]))
out.write('column name:'+', '.join([str(i) for i in col_name]))

inter = df1.select(col_name[0]).distinct()

if len(col_name)>1:
	for i in col_name[1:]:
		inter = inter.intersect(df1.select(i).distinct())

inter = [inter.collect()[i][0] for i in range(len(inter.collect()))]
print('the intersection is: '+', '.join([str(i) for i in inter]))
out.write('the intersection is:'+', '.join([str(i) for i in inter]))



out.close()

