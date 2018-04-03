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
    #print(type(s))
    #print(type(c[0]))
    for key in (set(s[1].keys()) | set(c[0].keys())):
        dist+=math.pow(s[1].get(key,0)-c[0].get(key,0),2)
    return dist

def find_avg(states):
    c={}
    for i in range(len(states)):
        for keys in (states[i][1].keys()):
            c.update({keys:c.get(keys,0)+states[i][1][keys]/len(states)})

    return c

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
rd=sc.textFile(file).flatMap(conv).reduceByKey(merge)
number=0
c=[]

while(number<int(k)):
    temp=rd.filter(lambda x:x[0]==str(centroids[number])).map(lambda x:x[1]).collect()
    c.append(temp)
    number+=1




state_list=rd.collect()
#print(len(state_list))
d=[]
store=0
clusters={}
for i in range(len(state_list)):
    for j in range(int(k)):
        #print("here is before " + str(type(c[j])))
        d.append(kmeans_dist(state_list[i],c[j]))
    dic_key=min_dist(d)
    #print(dic_key)
    clusters.setdefault(dic_key,[])
    clusters[dic_key].append(state_list[i])
    d=[]


new_clu=[]
old_clu=[]

file=open("test.txt","w", encoding="utf-8")
old_sig=str(new_clu)
new_sig=str(new_clu)
count=0
for keys in clusters:
    for element in range(len(clusters[keys])):
        file.write(str(clusters[keys][element][0])+ " ")
    file.write("\n")

while(True and count<10):
    for key in clusters.keys():
        for element in range(len(clusters[key])):
            old_clu.append(clusters[key][element][0])
    old_sig = str(old_clu)
    for keys in clusters:
        #c=[]
        c[keys]=[]
        c[keys]=[find_avg(clusters[keys])]
    clusters = {}
    for i in range(len(state_list)):
        for j in range(int(k)):
            #print(type(c[j]))
            d.append(kmeans_dist(state_list[i],c[j]))
        dic_key=min_dist(d)
        #print(dic_key)
        clusters.setdefault(dic_key,[])
        clusters[dic_key].append(state_list[i])
        d=[]
    for key in clusters.keys():
        for element in range(len(clusters[key])):
            new_clu.append(clusters[key][element][0])
    new_sig=str(new_clu)
    count+=1
    if(new_sig==old_sig):
        break
file.write("++++++++++++++++++++")
file.write("\n")
for keys in clusters:
    for element in range(len(clusters[keys])):
        file.write(str(clusters[keys][element][0]) + " ")
    file.write("\n")
file.close()




