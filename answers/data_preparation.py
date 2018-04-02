from pyspark.sql import SparkSession
import sys
import csv
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import Row
from pyspark import SparkContext, SparkConf
file=sys.argv[1]
def conv(line):
        elements=line.split(',')
        plant=elements[0]
        plant_dict={plant:1}
        states=[]
        size=len(elements)
        for i in range(1,size):
            tuples=(elements[i],plant_dict)
            states.append(tuples)
        return states

def merge(x, y):
    z = {**x, **y}
    return z
conf = SparkConf().setAppName('Kia_bigdata_lab').setMaster('local')
sc = SparkContext(conf=conf)
spark=SparkSession.builder.appName("lab3").getOrCreate()
rd=sc.textFile(file).flatMap(conv).reduceByKey(merge).