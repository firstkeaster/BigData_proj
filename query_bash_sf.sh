#!/bin/bash
# Ask the user for their name
echo Hello, which type of query do you want?
read varname
echo I got $varname
echo dataset?
read data
echo I got $data
module load python/gnu/3.4.4

if [ "$varname" == All_Unique ]
then
	/usr/bin/hadoop fs -rm -r -f "All_Unique.out"
	MAPPER=$(echo "All_Unique"/*map*.py)
	REDUCER=$(echo "All_Unique"/*reduce*.py)
	/usr/bin/hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files "All_Unique/" -mapper "$MAPPER" -reducer "$REDUCER" -input "$data" -output "All_Unique.out" 
	/usr/bin/hadoop fs -getmerge "All_Unique.out" "All_Unique/All_Unique.out"	
	#cat "task_ff/task_fftmp.out" | sort -n > "task_ff/task_fftmp.out"            
fi

if [ "$varname" == Columns_in_ex ]
then
	/usr/bin/hadoop fs -rm -r -f "Columns_in_ex.out"
	MAPPER=$(echo "Columns_in_ex"/*map*.py)
	REDUCER=$(echo "Columns_in_ex"/*reduce*.py)
	/usr/bin/hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files "Columns_in_ex/" -mapper "$MAPPER" -reducer "$REDUCER" -input "$data" -output "Columns_in_ex.out" 
	/usr/bin/hadoop fs -getmerge "Columns_in_ex.out" "Columns_in_ex/Columns_in_ex.out"	
	#cat "task_ff/task_fftmp.out" | sort -n > "task_ff/task_fftmp.out"            
fi

if [ "$varname" == Over_length ]
then
	/usr/bin/hadoop fs -rm -r -f "Over_length.out"
	MAPPER=$(echo "Columns_in_ex"/*map*.py)
	REDUCER=$(echo "Columns_in_ex"/*reduce*.py)
	/usr/bin/hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files "Over_length/" -mapper "$MAPPER" -reducer "$REDUCER" -input "$data" -output "Over_length.out" 
	/usr/bin/hadoop fs -getmerge "Over_length.out" "Over_length/Over_length.out"	
	#cat "task_ff/task_fftmp.out" | sort -n > "task_ff/task_fftmp.out"            
fi