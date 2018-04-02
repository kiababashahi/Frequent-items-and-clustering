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
it=model.freqItemsets
it.orderBy([func.size("items"), "freq"], ascending=[0,0])

# Display generated association rules.
as1=model.associationRules
as1.orderBy([func.size("antecedent"),"confidence"],ascending=[0,0])

joined=as1.join(it,as1.consequent==it.items)
# transform examines the input items against all the association rules and summarize the
# consequents as prediction
count=(df.count())
#print(count)
last=joined.withColumn("interest",func._create_function("abs")(joined["confidence"]-(joined["freq"]/count)))
last.orderBy([func.size("antecedent"),"interest"],ascending=[0,0]).show(int(n))
#model.transform(df).show()
