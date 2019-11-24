import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from tensorflow.keras.models import model_from_json
import tensorflow as tf
import numpy as np


def predict(data):
	json_file = open('model.json', 'r')
	loaded_model_json = json_file.read()
	json_file.close()
	loaded_model = model_from_json(loaded_model_json)
	# load weights into new model
	loaded_model.load_weights("model.h5")
	#print("Loaded model from disk")
	loaded_model.compile(loss="mean_squared_error", optimizer='adam')
	loaded_model.predict(data)

def createContext():
	sc = SparkContext(appName="Kafka2Spark")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc, 1)

	
	# Define KafkaConsumer
	kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'experiment':1})

	# Tried: mapPartitions, foreachRDD, flatmap, transform, foreachPartition
	# Processing
	messages = kafkaStream.map(lambda x: x[1]).map(lambda x: np.array(x).reshape((1,1,1)) if isinstance(x, float) else x)

	# Print to STDOUT
	messages.pprint(1)

	# Write on txt file
	messages.saveAsTextFiles('file:///home/BigData/Algorithmic-Trading/out_folder/out.txt')
	
	return ssc

if __name__ == '__main__':
	ssc = StreamingContext.getOrCreate('checkpoint', lambda: createContext())
	ssc.start()
	ssc.awaitTermination()

