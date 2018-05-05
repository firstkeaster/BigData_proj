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
echo '#find_format'
echo '#high_freq'
echo '#inter_column'
echo '#key_search'
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

#Task_inter

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
