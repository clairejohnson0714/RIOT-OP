#!/bin/python
import time
import paho.mqtt.client as paho


broker="192.168.122.12"
port=1883
def on_publish(client,userdata,result):
    print("data published \n")
    pass
client1= paho.Client("control1")                           #create client object
client1.connect(broker,port)

with open('/home/peng/storm/riot-bench/modules/tasks/src/main/resources/TAXI_sample_data_senml.csv') as fp:
    line = fp.readline()
    while line:
        ret= client1.publish("testTopic","taxi;myimage.jpg;"+line) 
        time.sleep(5)
        line = fp.readline() 
