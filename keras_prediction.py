import os
import numpy as np
from tensorflow.keras.models import model_from_json
from time import sleep
import time
import subprocess

if __name__ == '__main__':
	json_file = open('model.json', 'r')
	loaded_model_json = json_file.read()
	json_file.close()
	# load model architecture
	loaded_model = model_from_json(loaded_model_json)
	# load weights into new model
	loaded_model.load_weights("model.h5")
	loaded_model.compile(loss="mean_squared_error", optimizer='adam')
	
	index = 0
	curr_len = 0
	start_time = time.time()
	while index<50:
		directory = os.listdir("out_folder")
		new_len = len(directory)
		if new_len > curr_len:
			file = directory[index]
			path = "/".join(["out_folder", file, "part-00000"])
			try:
				with open(path, "r") as file:
					for line in file.readlines():
						data = np.array(float(line)).reshape((1,1,1))
						pred = loaded_model.predict(data)
						print(pred)
			except:
				pass
			finally:	
				index += 1
			curr_len = new_len

		
	elapsed_time = time.time() - start_time
	print('Elasped time: ' + str(elapsed_time) + '\tPredictions: ' + str(index) + '\tLatency: ' + str(elapsed_time/index))

