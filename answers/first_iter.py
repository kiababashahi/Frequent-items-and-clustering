from pyspark.sql import SparkSession
import sys
import csv
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import Row
from pyspark import SparkContext, SparkConf
import random
import sys
def conv(line):
    elements = line.split(',')
    plant = elements[0]
    plant_dict = {plant: 1}
    states = []
    size = len(elements)
    for i in range(1, size):
        tuples = (elements[i], plant_dict)
        states.append(tuples)
    return states
def merge(x, y):
    z = {**x, **y}
    return z

def difference(s,c):
    union=s.union(c)
    intersect = (s.intersection(c))
    diff=union.difference(intersect)
    return(len(diff))
def min_dist(c1,c2,c3):
    min=[]
    if(c1<c2):
        min.append(c1)
        min.append(1)
    else:
        min.append(c2)
        min.append(2)
    if(min[0]<c3):
        return min[1]
    else:
        min[0]=c3
        min[1]=3
        return min[1]





all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
           "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
           "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
           "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
           "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
           "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
           "yt", "dengl", "fraspm" ]

file=sys.argv[1]
k=sys.argv[2]
seed=sys.argv[3]
random.seed(int(seed))
centroids=[]
for i in range(int(k)):
    centroids.append(all_states[random.randint(0,len(all_states))])

conf = SparkConf().setAppName('Kia_bigdata_lab').setMaster('local')
sc = SparkContext(conf=conf)
class1=[]
class2=[]
class3=[]
spark=SparkSession.builder.appName("lab3").getOrCreate()
rd=sc.textFile(file).flatMap(conv).reduceByKey(merge)
c1=rd.filter(lambda x:x[0]==str(centroids[0])).map(lambda x:x[1]).collect()
c2=rd.filter(lambda x:x[0]==str(centroids[1])).map(lambda x:x[1]).collect()
c3=rd.filter(lambda x:x[0]==str(centroids[2])).map(lambda x:x[1]).collect()
set_C1=set(c1[0].keys())
set_C2=set(c2[0].keys())
set_C3=set(c3[0].keys())
state_list=rd.collect()
for i in range(len(state_list)):
    sa=set(state_list[i][1].keys())
    d1=difference(sa,set_C1)
    d2=difference(sa,set_C2)
    d3=difference(sa,set_C3)
#print(d1)
#print(d2)
#print(d3)
    distance=min_dist(d1,d2,d3)
    if(distance==1):
        class1.append(state_list[i][0])
    else :
        if(distance==2):
            class2.append(state_list[i][0])
        else:
            class3.append(state_list[i][0])
class1.sort()
class2.sort()
class3.sort()
#print(class1)
#print(class2)
#print(class3)
dic={class1[0]:class1,class2[0]:class2,class3[0]:class3}
i=0
for key in sorted(dic):
    print("* Class "+ str(i))
    print(dic[key])
    i+=1
'''
sa=set(s1[0].keys())
sb=set(s2[0].keys())
union=sa.union(sb)
intersect=(sa.intersection(sb))
dif=union.difference(intersect)
print(len(dif))'''