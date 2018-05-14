#!/bin/bash
echo '   _____                 __        __            '
echo '  / ___/____  ___  _____/ /_____ _/ /_____  _____'
echo '  \__ \/ __ \/ _ \/ ___/ __/ __ `/ __/ __ \/ ___/'
echo ' ___/ / /_/ /  __/ /__/ /_/ /_/ / /_/ /_/ / /    '
echo '/____/ .___/\___/\___/\__/\__,_/\__/\____/_/'
echo '    /_/'

echo ' _____        _           ____                          _____            _     _                         _ '
echo '|  __ \      | |         / __ \                        |  __ \          | |   | |                       | |'
echo '| |  | | __ _| |_ __ _  | |  | |_   _  ___ _ __ _   _  | |  | | __ _ ___| |__ | |__   ___   __ _ _ __ __| |'
echo '| |  | |/ _` | __/ _` | | |  | | | | |/ _ \ `__| | | | | |  | |/ _` / __| `_ \| `_ \ / _ \ / _` | `__/ _` |'
echo '| |__| | (_| | || (_| | | |__| | |_| |  __/ |  | |_| | | |__| | (_| \__ \ | | | |_) | (_) | (_| | | | (_| |'
echo '|_____/ \__,_|\__\__,_|  \___\_\\__,_|\___|_|   \__, | |_____/ \__,_|___/_| |_|_.__/ \___/ \__,_|_|  \__,_|'
echo '                                                 __/ |                                                     '
echo '                                                |___/                                                      '

echo '#Candidate Query:'
echo '#basic_stat'
echo '#find_format'
echo '#high_freq'
echo '#intersection_query'
echo '#key_search'
echo '#foreign_key'
echo '#Over_Length'
echo '#Columns_in_ex'
echo '#Value_Far'
echo '#SP_Char'
echo '#All_Unique'


echo -Hello, which type of query do you want?
read varname
echo -I got $varname
echo -Which dataset?
read data
echo -I got $data

if [ "$varname" == Over_Length ]
then
    echo -column?
    read column
    echo -your range?
    read range
fi

if [ "$varname" == Columns_in_ex ]
then
    echo -columns include?
    read include
    echo -columns exclude?
    read exclude
fi

if [ "$varname" == Value_Far ]
then
    echo -column?
    read column
    echo -your range?
    read range
fi

if [ "$varname" == intersection_query ]
then
    echo -columns?
    read column
fi

if [ "$varname" == foreign_key ]
then
    echo -dataset2?
    read data2
    echo -I got $data2
fi

module load python/gnu/3.4.4
module load spark/2.2.0

if [ "$varname" == find_format ]
then
    /usr/bin/hadoop fs -rm -r -f "task_fftmp.out"
    MAPPER=$(echo "task_ff"/*map*.py)
    REDUCER=$(echo "task_ff"/*reduce*.py)
    /usr/bin/hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files "task_ff/" -mapper "$MAPPER" -reducer "$REDUCER" -input "$data" -output "task_fftmp.out" 
    /usr/bin/hadoop fs -getmerge "task_fftmp.out" "task_ff/task_fftmp.out"	          
fi

if [ "$varname" == high_freq ]
then
    /usr/bin/hadoop fs -rm -r "out.csv"
    SPARKCODE=$(echo "task_freq/spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
    /usr/bin/hadoop fs -getmerge "out.csv" "task_freq/out.csv"        
fi

if [ "$varname" == basic_stat ]
then
	/usr/bin/hadoop fs -rm -r "task_stat.out"
	SPARKCODE=$(echo "task_stat/task_stat".py)
	spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
	/usr/bin/hadoop fs -getmerge "task_stat.out" "task_stat/task_stat.out"        
fi

if [ "$varname" == intersection_query ]
then
    /usr/bin/hadoop fs -rm -r "out.csv"
    SPARKCODE=$(echo "intersection_query/spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data" "$column"
    /usr/bin/hadoop fs -getmerge "out.csv" "intersection_query/out.csv"        
fi



if [ "$varname" == foreign_key ]
then
    /usr/bin/hadoop fs -rm -r "out.csv"
    SPARKCODE=$(echo "foreign_key/spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data" "$data2"
    /usr/bin/hadoop fs -getmerge "out.csv" "foreign_key/out.csv"        
fi

if [ "$varname" == key_search ]
then
    /usr/bin/hadoop fs -rm -r "out.csv"
    SPARKCODE=$(echo "key_search/key_search".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
    /usr/bin/hadoop fs -getmerge "out.csv" "key_search/out.csv"        
fi

if [ "$varname" == SP_Char_ ]
then
    /usr/bin/hadoop fs -rm -r "S_P_C_out.csv"
    SPARKCODE=$(echo "SP_Char/SP_Char_spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
    /usr/bin/hadoop fs -getmerge "S_P_C_out.csv" "SP_Char/S_P_C_out.csv"  
fi

if [ "$varname" == Columns_in_ex_ ]
then
    /usr/bin/hadoop fs -rm -r "C_I_E_out.csv"
    SPARKCODE=$(echo "Columns_in_ex/Columns_in_ex_spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data" "$include" "$exclude"
    /usr/bin/hadoop fs -getmerge "C_I_E_out.csv" "Columns_in_ex/C_I_E_out.csv"  
fi

if [ "$varname" == Value_Far_ ]
then
    /usr/bin/hadoop fs -rm -r "V_F_out.csv"
    SPARKCODE=$(echo "Value_Far/Value_Far_spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data" "$column" "$range"
    /usr/bin/hadoop fs -getmerge "V_F_out.csv" "Value_Far/V_F_out.csv"  
fi

if [ "$varname" == All_Unique_ ]
then
    /usr/bin/hadoop fs -rm -r "A_U_out.csv"
    SPARKCODE=$(echo "All_Unique/All_Unique_spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data"
    /usr/bin/hadoop fs -getmerge "A_U_out" "All_Unique/A_U_out.csv"  
fi

if [ "$varname" == Over_Length_ ]
then
    /usr/bin/hadoop fs -rm -r "O_L_out.csv"
    SPARKCODE=$(echo "Over_length/Over_length_spark".py)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data" "$column" "$range"
    /usr/bin/hadoop fs -getmerge "O_L_out.csv" "Over_length/O_L_out.csv"  
fi
#Task_etc

if [ "$varname" == SP_Char ]
then
    rm -r "S_P_C_out.csv"
    python SP_Char/SP_Char.py --filedir "$data"
fi

if [ "$varname" == Columns_in_ex ]
then
    rm -r "C_I_E_out.csv"
    python Columns_in_ex/Columns_in_ex.py --filedir "$data" --include "$include" --exclude "$exclude"
fi

if [ "$varname" == Value_Far ]
then
    rm -r "V_F_out.csv"
    python Value_Far/Value_Far.py --filedir "$data" --column "$column" --range "$range"
fi

if [ "$varname" == All_Unique ]
then
    rm -r "A_U_out.csv"
    python All_Unique/All_Unique.py --filedir "$data"
fi

if [ "$varname" == Over_Length ]
then
    rm -r "O_L_out.csv"
    python Over_length/Over_length.py --filedir "$data" --column "$column" --range "$range"
fi
