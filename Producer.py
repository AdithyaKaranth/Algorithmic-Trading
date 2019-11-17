# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 17:00:25 2019

@author: user
"""

from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas_datareader.data as web
import datetime

start = datetime.datetime(2010, 1, 1)
end = datetime.datetime(2017, 1, 11)

df = web.DataReader("AAPL", 'yahoo', start, end)


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))

for e in range(100):
    #dat = df[['High','Low']].iloc[e:(e+1)*10].to_json()
    dat = e
    data = {'number':dat}
    producer.send('numtest', value=data)
    sleep(1)
