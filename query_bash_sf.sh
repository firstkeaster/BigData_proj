#!/bin/bash
# Ask the user for their name
echo Hello, which type of query do you want?
read varname
echo I got $varname
echo dataset?
read data
echo I got $data

if [ "$varname" == Over_Length ]
then
    echo column?
    read column
fi

if [ "$varname" == Columns_in_ex ]
then
    echo columns include?
    read include
    echo columns exclude?
    read exclude
fi

if [ "$varname" == Value_Far ]
then
    echo column?
    read column
    echo your range?
    read range
fi

module load python/gnu/3.4.4



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
