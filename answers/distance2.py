from pyspark.sql import SparkSession
import sys
import csv
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import Row
from pyspark import SparkContext, SparkConf
file=sys.argv[1]
state1=sys.argv[2]
state2=sys.argv[3]
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
   # print(z['ca'])
    return z
conf = SparkConf().setAppName('Kia_bigdata_lab').setMaster('local')
sc = SparkContext(conf=conf)
spark=SparkSession.builder.appName("lab3").getOrCreate()
rd=sc.textFile(file).flatMap(conv).reduceByKey(merge)
s1=rd.filter(lambda x:x[0]==str(state1)).map(lambda x:x[1]).collect()
s2=rd.filter(lambda x:x[0]==str(state2)).map(lambda x:x[1]).collect()
#s1_dic=s1.parallelize.collect()
#s2=rd.filter(lambda x:x[0]==str(state2)).map(lambda x:x[1]).collect()
# I am going to merge the two rdd`s above which are now plant name and dict of 1 pairs
#print(len(s1[0].keys()))
#print(len(s2[0].keys()))
sa=set(s1[0].keys())
sb=set(s2[0].keys())
union=sa.union(sb)
intersect=(sa.intersection(sb))
dif=union.difference(intersect)
print(len(dif))