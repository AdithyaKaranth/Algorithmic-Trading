import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from tensorflow.keras.models import model_from_json
import tensorflow as tf
import numpy as np

def createContext():
	sc = SparkContext(appName="Kafka2Spark")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc, 0.5)
	
	# Define KafkaConsumer
	kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'experiment':1})

	# Processing
	messages = kafkaStream.map(lambda x: x[1])

	# Write on txt file
	messages.saveAsTextFiles('file:///home/BigData/Algorithmic-Trading/out_folder/out.txt')
	
	return ssc

if __name__ == '__main__':
	ssc = StreamingContext.getOrCreate('checkpoint', lambda: createContext())
	ssc.start()
	ssc.awaitTermination()

