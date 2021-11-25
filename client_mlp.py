import time
from pyspark import SparkContext
from pyspark.sql.functions import udf,variance
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import numpy as np
import nltk
from sklearn.neural_network import MLPClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from pyspark.sql.functions import when
import string
import pickle
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType


from sklearn.model_selection import train_test_split


from sklearn.feature_extraction.text import TfidfVectorizer
tfidf = TfidfVectorizer(sublinear_tf=True, min_df=5, norm='l2', encoding='latin-1', ngram_range=(1, 2), stop_words='english')

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
clf = MLPClassifier(random_state=1, max_iter = 5)
def plswork(rdd):
	from nltk.corpus import stopwords
	if not rdd.isEmpty():
		df = spark.read.json(rdd)
		
		n_df = df.select(udf1('Message').alias('udf1(Message)'), send_subject('Subject'), send_spam_ham('Spam/Ham'))
		df1 = n_df.withColumnRenamed("Spam/Ham","SpamHam").withColumnRenamed("udf1(Message)","Message")
		dfl = df1.withColumn("SpamHam", when(df1.SpamHam == "spam","1").when(df1.SpamHam == "ham","0").otherwise(df1.SpamHam))

		a = dfl.select('Message').collect()
		b = dfl.select('SpamHam').collect()
		messages = []
		target = []
		for row in a:
			messages.append(' '.join(row[0]))
		for row in b:
			target.append(' '.join(row[0]))
		
		y = np.array(target)
		tfidfconverter = TfidfVectorizer(max_features=1500, min_df=5, max_df=0.7, stop_words=stopwords.words('english'))
		X = tfidfconverter.fit_transform(messages).toarray()
		X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

		clf.partial_fit(X_train, y_train, classes = np.unique(y_train))
		
		print('Accuracy: ')
		print(clf.score(X_test, y_test))

		with open('spam_classifier_MLP', 'wb') as picklefile:
			print("done")
			pickle.dump(clf,picklefile)


data.foreachRDD(lambda rdd: plswork(rdd))



ssc.start()
ssc.awaitTermination()
