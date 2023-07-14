#!/bin/python
# This program for predict application. 
# produce input for MQTTsubscribe spout.
# Pay attention broker should be the server, running predict application to preduce message.

# how to check our message.
# mosquitto_sub -t "testTopic" -u "admin" -P "password"


import time
import paho.mqtt.client as paho


broker="worker1"
port=1883
def on_publish(client,userdata,result):
    print("data published \n")
    pass
client1= paho.Client("control1")                           #create client object
client1.connect(broker,port)

with open('/home/peng/storm/riot-bench/modules/tasks/src/main/resources/SYS_sample_data_senml.csv') as fp:
    line = fp.readline()
    while line:
        ret= client1.publish("testTopic","sys;myimage.jpg;"+line)  #publish
        time.sleep(10)
        line = fp.readline() 
