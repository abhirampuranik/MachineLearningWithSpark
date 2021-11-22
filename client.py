import time
from pyspark import SparkContext
from pyspark.sql.functions import udf,variance
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import numpy as np
import nltk

import string
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType


from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB

from sklearn.feature_extraction.text import TfidfVectorizer
tfidf = TfidfVectorizer(sublinear_tf=True, min_df=5, norm='l2', encoding='latin-1', ngram_range=(1, 2), stop_words='english')

sc = SparkContext("local[2]", "spam")
ssc = StreamingContext(sc,1)
spark = SparkSession(sc)

data = ssc.socketTextStream("localhost",6100)

classifier = MultinomialNB()

# nltk.download('stopwords')

def process_text(text):
	from nltk.corpus import stopwords
	nopunc = [char for char in text if char not in string.punctuation]
	nopunc = ''.join(nopunc)

	clean_words = [word for word in nopunc.split() if word.lower() not in stopwords.words('english')]

	return clean_words

def send_subject(x):
	return x
def send_spam_ham(x):
	return x

udf1 = udf(lambda x: process_text(x), ArrayType(StringType()))

def plswork(rdd):
	if not rdd.isEmpty():
		df = spark.read.json(rdd)
		#df.printSchema()
		# df.filter(df.select('*').isNull()).collect()
		# df.drop_duplicates()
		# df.na.drop().collect()

		# df.Message.foreach(process_text)
		# for row in df.head(df.count()):
		# 	row['Message'] = process_text(row['Message'])
		# df.


		name = 'Message'
		
		# df.select(udf1(column).alias(column) if column == name else column for column in df.columns).show()
		# new_df = select(*[udf(column).alias(name) if column == name else column for column in df.columns])
		df.select(udf1('Message').alias('udf1(Message)'), send_subject('Subject'), send_spam_ham('Spam/Ham')).show()
		# new_df.show()
		# a = np.array(.select('*').collect())
		# print(a)
		



data.foreachRDD(lambda rdd: plswork(rdd))

ssc.start()
ssc.awaitTermination()
	
