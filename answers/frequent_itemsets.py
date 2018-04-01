from pyspark.sql import SparkSession
import sys
import csv
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import Row
from pyspark import SparkContext, SparkConf
import pyspark.sql.functions as func
file=sys.argv[1]
n=sys.argv[2]
s=sys.argv[3]
c=sys.argv[4]
conf = SparkConf().setAppName('Kia_bigdata_lab').setMaster('local')
sc = SparkContext(conf=conf)
spark=SparkSession.builder.appName("lab3").getOrCreate()
rd=sc.textFile(file).map(lambda x: Row((x.split(','))[0],x.split(',')[1:]))
df=rd.toDF(["items","plant"]).withColumn("id",monotonically_increasing_id())
df=df[["id","items","plant"]]

fpGrowth = FPGrowth(itemsCol="plant", minSupport=float(s), minConfidence=float(c))
model = fpGrowth.fit(df)

# Display frequent itemsets.
ml=model.freqItemsets
ml.orderBy([func.size("items"), "freq"], ascending=[0,0]).show(int(n))

# Display generated association rules.
#model.associationRules.show()

# transform examines the input items against all the association rules and summarize the
# consequents as prediction
#model.transform(df).show()
