import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

def createContext():
	sc = SparkContext(appName="Kafka2Spark")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc, 5)

	# Define KafkaConsumer
	kafkaStream = KafkaUtils.createStream(ssc, 'sandbox-hdp.hortonworks.com:6667', 'spark-streaming', {'numtest':1})

	# Processing
	messages = kafkaStream.map(lambda x: x[1])

	#count_this_batch = messages.count().map(lambda x: "This batch has {} messages.".format(x))
	#count_window = messages.countByWindow(60, 5).map(lambda x: "Number of messages (rolling 1 minute): {}".format(x))

	# Count the number of reviews with the word great
	count_great_this_batch = messages.filter(lambda x: "great" in x).count().map(lambda x: "This batch has {} messages with the word, great".format(x))
	count_great_window = messages.filter(lambda x: "great" in x).countByWindow(60, 5).map(lambda x: "Number of messages (rolling 1 minute) with the word, great: {}".format(x))

	# Print to STDOUT
	messages.pprint(5)
	#count_this_batch.pprint(5)
	#count_window.pprint(5)
	count_great_this_batch.pprint(5)
	count_great_window.pprint(5)

	return ssc

if __name__ == '__main__':
	ssc = StreamingContext.getOrCreate('checkpoint', lambda: createContext())
	ssc.start()
	ssc.awaitTermination()
