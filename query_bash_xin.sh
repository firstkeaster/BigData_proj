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

# Ask the user for their name

echo '#intersection_query'
echo '#foreign_key'





# Ask the user for their name
echo -Hello, which type of query do you want?
read varname
echo -I got $varname
echo -dataset?
read data
echo -I got $data


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
#Running query
#Task_Freq


#Task_inter

if [ "$varname" == intersection_query ]
then
    /usr/bin/hadoop fs -rm -r "out.csv"
    SPARKCODE=$(echo "intersection_query/spark".py)
    start=$(date +%s.%N)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data" "$column"
    end=$(date +%s.%N)
    /usr/bin/hadoop fs -getmerge "out.csv" "intersection_query/out.csv"        
fi



if [ "$varname" == foreign_key ]
then
    /usr/bin/hadoop fs -rm -r "out.csv"
    SPARKCODE=$(echo "foreign_key/spark".py)
    start=$(date +%s.%N)
    spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python "$SPARKCODE" "$data" "$data2"
    end=$(date +%s.%N)
    /usr/bin/hadoop fs -getmerge "out.csv" "foreign_key/out.csv"        
fi


runtime=$(python -c "print(${end} - ${start})")
echo "Runtime was $runtime"
