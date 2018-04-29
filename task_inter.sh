#!/bin/bash
# Ask the user for their name
echo Hello, which type of query do you want?
read varname
echo I got $varname
echo dataset?
read data
echo I got $data
module load python/gnu/3.4.4

if [ "$varname" == inter_column ]
then
	/usr/bin/hadoop fs -rm -r "out.csv"
	SPARKCODE=$(echo "inter_column/inter_column".py)
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
	/usr/bin/hadoop fs -getmerge "out.csv" "inter_column/out.csv"        
fi

if [ "$varname" == key_search ]
then
	/usr/bin/hadoop fs -rm -r "out.csv"
	SPARKCODE=$(echo "key_search/key_search".py)
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
	/usr/bin/hadoop fs -getmerge "out.csv" "key_search/out.csv"        
fi

