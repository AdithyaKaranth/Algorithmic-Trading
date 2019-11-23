#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  9 15:49:04 2019

@author: dyb
"""

from kafka import KafkaProducer
import sys
import json
from kafka.errors import KafkaError
import csv
#import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                           value_serializer=lambda m: json.dumps(m).encode('ascii'))

topic ="stock"


with open(sys.argv[1], newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')

    for row in reader:
        future = producer.send(topic, row[1])
        #time.sleep(1)

        try:
            future.get(timeout=10)
        except KafkaError as e:
            print(e)
            break
