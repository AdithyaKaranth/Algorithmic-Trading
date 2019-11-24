
# how to keep reading new files in the directory to make prediction
import os
import numpy as np
from tensorflow.keras.models import model_from_json
from time import sleep
import subprocess

if __name__ == '__main__':
	json_file = open('model.json', 'r')
	loaded_model_json = json_file.read()
	json_file.close()
	loaded_model = model_from_json(loaded_model_json)
	# load weights into new model
	loaded_model.load_weights("model.h5")
	#print("Loaded model from disk")
	loaded_model.compile(loss="mean_squared_error", optimizer='adam')
	
	'''cmd = 'hadoop fs -ls /user/BigData/out_folder'
	files = subprocess.check_output(cmd, shell=True).strip().split('\n')
	for line in files:
		try:
			cmd = 'hadoop fs -cat '+ line.split()[7]+'/part-00000'
			output = subprocess.check_output(cmd,shell= True)
			print(output)
		except:
			pass'''
	index = 0
	curr_len = 0
	#while True:
	start_time = time.time()
	while index <10:
		#print(index)
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
				#print("Except")
				pass
			finally:
				#print("Added")	
				index += 1

			curr_len = new_len
		else:
			print("Slept")
			sleep(0.1)

	elapsed_time = time.time() - start_time
	print('elaspsed time: ' + str(elapsed) + "/tNumber of Predictions: " + str(index) + "/tLatency: " +str(index/elapsed))


