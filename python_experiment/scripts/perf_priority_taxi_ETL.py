#!/bin/python3
import requests
import sys
import time
import os
import json
import timeit
import redis


# step 2: define our connection information for Redis
# Replaces with your configuration information
redis_host = "redis"
redis_port = 6379
redis_password = ""

bucket = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
 1024, 2048, 4096, 8192, 16384, 32768, 60000, 120000]

def get_power(number):
     for i in range(len(bucket)):
         if number <= bucket[i]: 
             return i 
def connectRedis():
    r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    return r

def calculate_latency(appName="ETLTopologySYS", currentTime=time.time()):
    throughput = 0
    tail_latency = {}
    avg_latency = {}
    median_latency = {}
    priority_latency = {1: [], 2: [], 3: []}
    result = {}
    
    try:
        r = connectRedis()
        timestamp = currentTime
        remain = timestamp % 60 
        timestamp = timestamp - remain - 60

        spout = r.hgetall(appName+"_spout_"+str(timestamp))
        throughput = len(spout)
        sink = r.hgetall(appName+"_sink_"+str(timestamp))
        #sink2 = r.hgetall(appName+"_sink_"+str(timestamp+60))
        #sink3 = r.hgetall(appName+"_sink_"+str(timestamp-60))
        #sink = {**sink, **sink2, **sink3}

        print(len(spout), len(sink))
        for key, value in spout.items():
            priority_value = value.split("_")
            #print("**********",priority_value[0])
            if int(priority_value[0]) > 40000 or int(priority_value[0]) < 20000:
                #print("in if")
                continue
            word = key.split("_")
            #print("word[1]",word[1])
            if word[1] in sink:
                #print("####",int(sink[word[1]]),int(priority_value[0]))
                try:
                    priority = int(priority_value[1])
                except Exception as e:
                    print("Exception ", key, priority_value)
                if priority in priority_latency:
                    priority_latency[priority].append(int(sink[word[1]]) - int(priority_value[0]))
        #print("######",priority_latency) 
        for p, latencies in priority_latency.items():
            latencies = sorted(latencies)
            if len(latencies) > 0:
                tail_latency[p] = latencies[int(len(latencies) * 0.95)]
                median_latency_index = int(len(latencies) * 0.50)
                median_latency[p] = latencies[median_latency_index]
                avg_latency[p] = sum(latencies) / len(latencies)
                throughput = len(latencies)
                result[p] = (tail_latency[p], median_latency[p], avg_latency[p], throughput)
            else:
                result[p] = (0, 0, 0, throughput)
   
        return result
    
    except Exception as e:
        print("Exception", e)

def statistic_info(app_name, currentTime):
    result = {}
    print("\nstart experiment------------------------------", app_name)
    latency_results = calculate_latency(app_name, currentTime)
    
    if latency_results:
        for priority, latency_info in latency_results.items():
            result['latency_priority' + str(priority)] = latency_info[0]
            result['median_latency_priority' + str(priority)] = latency_info[1]
            result['avg_latency_priority' + str(priority)] = latency_info[2]
            result['throughput_priority' + str(priority)] = latency_info[3]
    
    print("result is ", result)
    return result


if __name__ == '__main__':
    filePath = "/home/cc/storm/riot-bench/output/skopt_input_ETLTopologySYS.csv"
    if not os.path.isfile(filePath):
        with open(filePath, 'a') as f:
            f.write("latency_priority1,median_latency1,avg_latency_priority1,throughput_priority1,latency_priority2,median_latency2,avg_latency_priority2,throughput_priority2,latency_priority3,median_latency3,avg_latency_priority3,throughput_priority3\n")

    while True:
        currentTime = int(time.time())
        currentTime = currentTime - currentTime % 60
        data = {}
        for appName in ["ETLTopologySYS"]:
            start = timeit.default_timer()
            data[appName] = statistic_info(appName, currentTime)
            stop = timeit.default_timer()

        if data and all(data[appName] for appName in data):
            r = connectRedis()
            r_data = r.get("perf-" + str(currentTime))
            if r_data:
                data = json.loads(r_data)

            with open(filePath, 'a') as f:
                row = [
                    data[appName].get('latency_priority1', 0),
                    data[appName].get('median_latency_priority1', 0),
                    data[appName].get('avg_latency_priority1', 0),
                    data[appName].get('throughput_priority1', 0),
                    data[appName].get('latency_priority2', 0),
                    data[appName].get('median_latency_priority2', 0),
                    data[appName].get('avg_latency_priority2', 0),
                    data[appName].get('throughput_priority2', 0),
                    data[appName].get('latency_priority3', 0),
                    data[appName].get('median_latency_priority3', 0),
                    data[appName].get('avg_latency_priority3', 0),
                    data[appName].get('throughput_priority3', 0)
                ]
                f.write(','.join(map(str, row)) + '\n')

        #time.sleep(60)
        break
