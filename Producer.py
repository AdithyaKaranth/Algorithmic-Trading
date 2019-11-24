from kafka import KafkaProducer
from datetime import datetime
from time import sleep
import sys

def connect_kafka_producer():
	producer = None
	try:
		producer = KafkaProducer(bootstrap_servers='localhost:9092')
	except Exception as ex:
		print("Exception while connecting to Kafka")
		print(ex)
	finally:
		return producer

def publish_message(producer_instance, topic, key, value):
	#try:
		key_bytes = bytes(key)
		value_bytes = bytes(value)
		producer_instance.send(topic, key=key_bytes, value=value_bytes)
		producer.flush()
		print('Message published succesfully at {}'.format(datetime.now()))
#except Exception as ex:
		#print("Exception in publishing message")
		

if __name__ == '__main__':
	"""
	Read a text or csv file and publish message every second
	"""
	file = str(sys.argv[1])
	topic = str(sys.argv[2])
	print(file)
	print(topic)
	data = open(file)
	producer = connect_kafka_producer()
	for line in data.readlines():
		publish_message(producer, topic, "line", line)
		sleep(1)
