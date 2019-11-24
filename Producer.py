from kafka import KafkaProducer
from datetime import datetime
from time import sleep
import sys
import subprocess

def connect_kafka_producer():
	"""
	Initializes a Kafka producer
	"""
	producer = None
	try:
		producer = KafkaProducer(bootstrap_servers='localhost:9092')
	except Exception as ex:
		print("Exception while connecting to Kafka")
		print(ex)
	finally:
		return producer

def publish_message(producer_instance, topic, key, value):
	"""
	Publishes the message to a Kafka broker. 
	Default localhost of Kafka broker is 2181.

	@Param:topic
	The Kafka topic we are interested publishing on. 

	@Param:key
	The key for the key-value pair

	@Param: value
	The value for the key-value pair


	"""
	try:
		key_bytes = bytes(key)
		value_bytes = bytes(value)
		producer_instance.send(topic, key=key_bytes, value=value_bytes)
		producer.flush()
		print('Message published succesfully at {}'.format(datetime.now()))
	except Exception as ex:
		print("Exception in publishing message")
		

if __name__ == '__main__':
	"""
	Read a text or csv file and publish message every second
	"""
	topic = str(sys.argv[1])
	cat = subprocess.Popen(["hadoop","fs","-cat","/user/BigData/x_test.csv",],stdout=subprocess.PIPE)
	producer = connect_kafka_producer()
	for line in cat.stdout:
		publish_message(producer,topic,"line",line)