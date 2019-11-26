#start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties 

#start kafka
bin/kafka-server-start.sh config/server.properties 

#Start a worker
faust -A stock worker -l info

#run the program
python producer.py test.txt






