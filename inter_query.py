import sys
import numpy as np 
from pyspark import SparkContext
from csv import reader

def load_data(file_path, file):
    path = file_path + '/' +file
    data = spark.read.parquet(path)
    return data

def query(data1, col1, data2, col2):
    return (data1.select(col1)).intersect(data2.select(col2))
    

