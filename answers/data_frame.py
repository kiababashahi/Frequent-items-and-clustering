from pyspark.sql import SparkSession
import sys


from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import Row
from pyspark import SparkContext, SparkConf
file=sys.argv[1]
n=sys.argv[2]
conf = SparkConf().setAppName('Kia_bigdata_lab').setMaster('local')
sc = SparkContext(conf=conf)
spark=SparkSession.builder.appName("lab3").getOrCreate()
rd=sc.textFile(file).map(lambda x: Row((x.split(','))[0],x.split(',')[1:]))
df=rd.toDF(["plant","items"]).withColumn("id",monotonically_increasing_id())
df=df.select("id","plant","items").show(int(n))