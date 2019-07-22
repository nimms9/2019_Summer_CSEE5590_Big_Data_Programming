from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple

import os
os.environ["SPARK_HOME"] = "C:\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\winutils"

sc = SparkContext(appName="Lab 4")

# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

ssc = StreamingContext(sc, 3)

Tweet = namedtuple("Data", ("tag", "count"))

# Split each line into words and use map reduce to count occurance of token then print word count
ssc.socketTextStream("localhost", 5000).flatMap(lambda line: line.split(" ")).map(lambda word: (word.lower(), 1)).reduceByKey(lambda x, y: x + y).map(lambda rec: Tweet(rec[0], rec[1])).pprint()

# Start spark streaming
ssc.start()
ssc.awaitTermination()