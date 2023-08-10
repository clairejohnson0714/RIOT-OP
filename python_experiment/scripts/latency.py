import sys
import time
import os
import json
import timeit

def calculate_latency(appName):
    timestamp = int((time.time() * 1000) - 59000)
    print(timestamp,"*******************")
    input_filename_sink = "/home/cc/storm/riot-bench/output/sink-ETLTopologySYS-SENML-6.6667.log"
    input_filename_spout = "/home/cc/storm/riot-bench/output/spout-ETLTopologySYS-SENML-6.6667.log1175000000001"
    output_filename = "/home/cc/storm/riot-bench/output/one-minute.log"
    latency1 = 0

    # Read sink data and store in a new file for one minute
    data_to_cut = []
    with open(input_filename_sink, "r") as f_sink, open(output_filename, "w") as dest_file:
        print("Opened sink file")
        for i, line_sink in enumerate(f_sink):
            if i == 0 and line_sink.strip() == "":
                print("Sink file is empty. Skipping the first line.")
                continue

            ts_sink, process_id_sink = line_sink.strip().split(',')
            process_id_sink = int(process_id_sink)
            ts_sink = int(ts_sink)
            if ts_sink < timestamp + 60000:
                dest_file.write(f"{ts_sink},{process_id_sink}\n")
                data_to_cut.append(line_sink)
            else:
                break

    print("One-minute data stored in", output_filename)

    # Remove the data from the sink file using temporary file and rename
    with open(input_filename_sink, "r") as f, open(input_filename_sink + ".temp", "w") as fw:
        for line in f:
            if line not in data_to_cut:
                fw.write(line)

    os.remove(input_filename_sink)
    os.rename(input_filename_sink + ".temp", input_filename_sink)
    print("Data cut from the sink file.")


    # Calculate latency by matching process IDs in sink and spout data
    latency = []
    with open(input_filename_spout, "r") as f_spout, open(output_filename, "r") as f_one_minute:
        print("Opened spout file")
        for line_sink in f_one_minute:
            ts_sink, process_id_sink = line_sink.strip().split(',')
            process_id_sink = int(process_id_sink)
            ts_sink = int(ts_sink)
            f_spout.seek(0)  # Reset the spout file pointer to the beginning
            for line_spout in f_spout:
                ts_spout, _, process_id_spout = line_spout.strip().split(',')[0:3]
                process_id_spout = int(process_id_spout)
                ts_spout = int(ts_spout)
                if process_id_sink == process_id_spout:
                    latency1 = ts_sink - ts_spout
                    print("Latency for process_id", process_id_sink, "is", latency1)
                    latency.append(latency1)
                    break

    # Check if latency list is not empty before calculating tail_latency
    if len(latency) > 0:
        latency = sorted(latency)
        throughput = len(latency)
        tail_latency = latency[int(len(latency) * 0.95)]
    else:
        throughput = 0
        tail_latency = 0

    print("Latency calculation completed. Throughput:", throughput, "Tail Latency (95th percentile):", tail_latency)
    return tail_latency, len(latency)

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
