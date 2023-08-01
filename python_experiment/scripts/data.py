import requests
import sys
import time
import os
import json
import timeit

def calculate_latency(appName):
    latency_histogram = {}  # Dictionary to hold the latency histogram
    tail_latency = 0
    latency1=0
    latency=[]
    timestamp = int((time.time() * 1000) - 59000)
    input_filename_sink = "/home/cc/storm/riot-bench/output/sink-ETLTopologySYS-SENML-6.6667.log"
    input_filename_spout = "/home/cc/storm/riot-bench/output/spout-ETLTopologySYS-SENML-6.6667.log1297000000008"

    # Read sink data and store in a dictionary {process_id: timestamp}
    with open(input_filename_sink, "r") as f_sink: 
        for line_sink in f_sink:
            ts_sink, process_id_sink = line_sink.strip().split(',')
            process_id_sink = int(process_id_sink)
            ts_sink = int(ts_sink)
            if ts_sink < timestamp + 60000:
                with open(input_filename_spout, "r") as f_spout:
                    for line_spout in f_spout:
                        ts_spout, _, process_id_spout = line_spout.strip().split(',')[0:3]
                        process_id_spout = int(process_id_spout)
                        ts_spout = int(ts_spout)

                        # Check if the process IDs match between sink and spout
                        if process_id_sink == process_id_spout:
                            latency1 = ts_sink - ts_spout
                            latency.append(latency1)
                            break
# Calculate the throughput and latency statistics
throughput = len(latency)
total_latency_count = len(latency)

# Sort the latency list and calculate tail latency (e.g., 95th percentile)
latency = sorted(latency)
tail_latency_index = int(total_latency_count * 0.95)
tail_latency = latency[min(tail_latency_index, total_latency_count - 1)]

    # Delete the processed data from the sink file
with open(input_filename_sink, "r") as f:
    lines = f.readlines()
with open(input_filename_sink, 'w') as fw:
    for line1 in lines:
        ts, process_id = line1.strip().split(',')
        if int(ts) > timestamp - 240000:
            fw.write(line1)

    print("Deleted")
    return tail_latency, total_latency_count

while True:
    result = {}
    start = timeit.default_timer() 
    result['latency'], result['throughput'] = calculate_latency("ETLTopologySYS")
    with open('/home/cc/storm/riot-bench/output/skopt_input_ETLTopologySys.txt', 'a+') as f: 
        f.write(json.dumps(result) + "\n")
    stop = timeit.default_timer()
    time_taken = stop - start
    print("Time taken:", time_taken)
    minute = 60 - (time_taken / 1000000)
    time.sleep(minute)