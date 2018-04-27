#!/bin/bash
# Ask the user for their name
echo Hello, which type of query do you want?
read varname
echo I got $varname
echo dataset?
read data
echo I got $data
module load python/gnu/3.4.4

/usr/bin/hadoop fs -rm -r -f "task_fftmp.out"


if [ "$varname" == find_format ]
then
	/usr/bin/hadoop fs -rm -r -f "task_fftmp.out"
	MAPPER=$(echo "task_ff"/*map*.py)
	REDUCER=$(echo "task_ff"/*reduce*.py)
	/usr/bin/hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files "task_ff/" -mapper "$MAPPER" -reducer "$REDUCER" -input "$data" -output "task_fftmp.out" 
	/usr/bin/hadoop fs -getmerge "task_fftmp.out" "task_ff/task_fftmp.out"	
	#cat "task_ff/task_fftmp.out" | sort -n > "task_ff/task_fftmp.out"            
fi

if [ "$varname" == high_freq ]
then
	/usr/bin/hadoop fs -rm -r "out.csv"
	SPARKCODE=$(echo "task_freq/spark".py)
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
	/usr/bin/hadoop fs -getmerge "out.csv" "task_freq/out.csv"        
fi