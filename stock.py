#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 15:36:36 2019

@author: dyb
"""

import faust
import numpy as np
from tensorflow.keras.models import model_from_json


json_file = open('/Users/dyb/Spyder/Big Data/model.json', 'r')
loaded_model_json = json_file.read()
json_file.close()
loaded_model = model_from_json(loaded_model_json)
# load weights into new model
loaded_model.load_weights("/Users/dyb/Spyder/Big Data/model.h5")
#print("Loaded model from disk")


loaded_model.compile(loss='mean_squared_error', optimizer='adam', metrics=['accuracy'])



app = faust.App('stock-app', broker='kafka://localhost',
                value_serializer='raw',)
topic = app.topic('stock', value_type=bytes)



@app.agent(topic)
async def hello(stream):
    async for obj in stream:
        
        price = float(eval(obj.decode("utf-8")))
        prediction = loaded_model.predict(np.array(price).reshape(1,1,1))
        print(prediction)
        
        #Trading algorithm insert here!

if __name__ == '__main__':
    app.main()