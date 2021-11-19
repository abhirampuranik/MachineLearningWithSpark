import time
from pyspark import SparkContext
from pyspark.sql.functions import udf,variance
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession





sc = SparkContext("local[2]", "spam")
ssc = StreamingContext(sc,1)
spark = SparkSession(sc)

data = ssc.socketTextStream("localhost",6100)

def plswork(rdd):
	df = spark.read.json(rdd)
	#df.printSchema()
	print(df.count())
	# df.show( )
	# for i in range(df.count()):

	# print(df.columns)
	#processed_df = tp.preProcessText(df)
	#df.createOrReplaceTempView("viewdf")
	#SQL statements can be run by using the sql methods provided by spark
	#df1 = spark.sql("SELECT * FROM viewdf")
	#df1.show()
	
data.foreachRDD(lambda rdd: plswork(rdd))

ssc.start()
ssc.awaitTermination()
	
