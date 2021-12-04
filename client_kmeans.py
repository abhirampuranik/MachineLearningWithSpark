import time
from pyspark import SparkContext
from pyspark.sql.functions import udf,variance
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import numpy as np
import nltk
import json
from sklearn.cluster import MiniBatchKMeans
from sklearn.model_selection import train_test_split
from pyspark.sql.functions import when
import string
import pickle
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from sklearn.model_selection import train_test_split
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from sklearn.feature_extraction.text import HashingVectorizer


sc = SparkContext("local[2]", "spam")
ssc = StreamingContext(sc,1)
spark = SparkSession(sc)

data = ssc.socketTextStream("localhost",6100)

#classifier = LinearRegression()

nltk.download('stopwords')

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
clf = MiniBatchKMeans(n_clusters=2,random_state=0,batch_size=1000)
def streamread(rdd):
	global clf
	from nltk.corpus import stopwords
	if not rdd.isEmpty():
		Stream=rdd.collect()
		vals=[i for val in Stream for i in list(json.loads(val).values())]

		schema=StructType([
			StructField('Subject',StringType(),False),
			StructField('Message',StringType(),False),
			StructField('Spam/Ham',StringType(),False)
		])
		df=spark.createDataFrame((Row(**d) for d in vals),schema)
		df=df['Message','Spam/Ham', 'Subject']


		n_df = df.select(udf1('Message').alias('udf1(Message)'), send_subject('Subject'), send_spam_ham('Spam/Ham'))
		df1 = n_df.withColumnRenamed("Spam/Ham","Spamham")
		df2 = df1.withColumnRenamed("udf1(Message)","Message")
		dfl = df2.withColumn("Spamham", when(df1.Spamham == "spam","1").when(df1.Spamham == "ham","0").otherwise(df1.Spamham))

		a = dfl.select('Message').collect()
		b = dfl.select('Spamham').collect()		
		
		messages = []
		target = []
		for row in a:
			messages.append(' '.join(row[0]))
		for row in b:
			target.append(' '.join(row[0]))
		
		y = np.array(target)
		vectorizer = HashingVectorizer(n_features=2**12,norm = 'l1',stop_words='english', alternate_sign=False)
		X = vectorizer.fit_transform(messages).toarray()
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

		clf.partial_fit(X_train)
		
		print('Accuracy: ')
		print(clf.score(X_test))

		with open('spam_classifier_Kmeans', 'wb') as picklefile:
			print("done")
			pickle.dump(clf,picklefile)


data.foreachRDD(lambda rdd: streamread(rdd))



ssc.start()
ssc.awaitTermination()
