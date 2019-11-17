# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 17:04:19 2019

@author: user
"""

from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    print('{} printed'.format(message))
    
