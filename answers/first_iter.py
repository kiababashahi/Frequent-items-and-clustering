from pyspark.sql import SparkSession
#####use arrays for classes
import sys
import math
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
def min_dist(d):
    min_index=-1
    min=math.inf
    for i in range(len(d)):
        if(d[i]<min):
            min=d[i]
            min_index=i
    return min_index

def kmeans_dist(s,c):
    #print(s[1].keys())
    #print(c[1].keys())
    dist=0
    for key in (set(s[1].keys())|set(c[1].keys())):
        dist+=math.pow(s[1].get(key,0)-c[1].get(key,0),2)
    return dist




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



centroids=random.sample(all_states, int(k))

conf = SparkConf().setAppName('Kia_bigdata_lab').setMaster('local')
sc = SparkContext(conf=conf)
spark=SparkSession.builder.appName("lab3").getOrCreate()
rd=sc.textFile(file).flatMap(conv).reduceByKey(merge).filter(lambda x:x[0] in all_states)
number=0
c=[]

while(number<int(k)):
    temp=rd.filter(lambda x:x[0]==str(centroids[number])).map(lambda x:x[1]).collect()
    c.append(temp)
    number+=1
set_C=[]

'''
for i in range(int(k)):
    clusters.append(i)
classes.fromkeys()

'''
for i in range(int(k)):
    set_C.append(set(c[i][0].keys()))
state_list=rd.collect()
#print(len(state_list))
d=[]
store=0
clusters={}
for i in range(len(state_list)):
    sa = set(state_list[i][1].keys())
    for j in range(int(k)):
        d.append(difference(sa,set_C[j]))
    dic_key=min_dist(d)
    #print(dic_key)
    clusters.setdefault(dic_key,[])
    clusters[dic_key].append(state_list[i][0])
    d=[]

for keys in clusters.keys():
   clusters[keys].sort()

representations={}
count=0
for i in range(len(clusters)):
    representations.setdefault(clusters[i][0], clusters[i])

for key in sorted(representations):
    print("* Class " + str(count))
    text=""
    for element in representations[key]:
        text+=str(element+" ")
    print(text)
    count+=1


#print(difference(set(state_list[1][1].keys()),set(state_list[2][1].keys())))


