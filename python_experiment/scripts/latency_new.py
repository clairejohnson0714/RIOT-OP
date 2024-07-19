import sys
import time
import timeit
import linecache
import csv
import os

# You have to kill the script in order to see data in the csv file
# Usage: python latency_new.py SPOUT_FILE SINK_FILE
# Spout Tuple: [time, "MSGID", pid, priority]
# Sink Tuple: [time, pid]
HOME = "/home/cc"
SPOUT_FILE = f"{HOME}/storm/riot-bench/output/{sys.argv[1]}"
SINK_FILE = f"{HOME}/storm/riot-bench/output/{sys.argv[2]}"
OUTPUT_FILE = f"{HOME}/storm/riot-bench/output/data.csv"

print(f"Spout File: {SPOUT_FILE}")
print(f"Sink File: {SINK_FILE}")
print(f"Program Output File: {OUTPUT_FILE}")

# Delete the previous output file if it exists
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)
    print(f"Previous output file {OUTPUT_FILE} deleted.")

# Algorithm Idea
# Use hash table of sink data for much faster O(n) searching - overwhelming majority of processes will exist in the hash table
# If a process does not exist in the hash table - use slower O(n^2) exhaustive search algorithm to find it instead
# If it is still not found at this point, then the process was dropped and did not reach the sink

# Given spout file stream and starting line number, get 1 minute's worth of tuples
def get_spout_tuples(spout_file, spout_line):
    spout_tuples = []
    tuple = linecache.getline(spout_file, spout_line).strip()
    if tuple == "":
        linecache.checkcache() # Update linecache's files
        tuple = linecache.getline(spout_file, spout_line).strip()
    # For each new tuple, check time and collect it if it's within 1 minute
    while tuple != "":
        time, msgid, pid, priority = tuple.split(",")
        time = int(time)
        pid = int(pid)
        priority = int(priority)
        if len(spout_tuples) == 0:
            start_time = time
        if time <= start_time + 60000: # Not doing start_time <= time as some data are delayed slightly
            spout_tuples.append([time, pid, priority])
            spout_line += 1
            tuple = linecache.getline(spout_file, spout_line).strip()
        else:
            break
    return spout_tuples, spout_line

# Given sink file stream and starting line number, get/update hash table of sink tuples sorted by pid number
def get_sink_tuples(sink_file, sink_line, sink_tuples):
    tuple = linecache.getline(sink_file, sink_line).strip()
    while tuple != "":
        time, pid = map(int, tuple.split(","))
        sink_tuples[pid] = time
        sink_line += 1
        tuple = linecache.getline(sink_file, sink_line).strip()
    return sink_tuples, sink_line

# Given 1 minute's worth of tuples and sink tuple hash table, calculate throughput and 95th percentile tail latency
def calculate_latency(spout_tuples, sink_tuples):
    latencies = {1: [], 2: [], 3: []}  # Dictionary to store latencies based on priority
    # For each collected tuple, find its end time (by accessing the hash table via its pid) and calculate latency
    for time_spout, pid, priority in spout_tuples:
        if pid in sink_tuples:
            time_sink = sink_tuples[pid]
            latency = time_sink - time_spout
            latencies[priority].append(latency)  # Append latency to the respective priority list
            del sink_tuples[pid]
    throughput = {1: len(latencies[1]), 2: len(latencies[2]), 3: len(latencies[3])}
    tail_latency = {1: sorted(latencies[1])[int(len(latencies[1]) * 0.95)] if latencies[1] else 0,
                    2: sorted(latencies[2])[int(len(latencies[2]) * 0.95)] if latencies[2] else 0,
                    3: sorted(latencies[3])[int(len(latencies[3]) * 0.95)] if latencies[3] else 0}
    return throughput, tail_latency

print("Sleeping for 1 minute to allow data to come in zzz...") # Data starts being transmitted 60 to 90 seconds after topology start
time.sleep(60)
spout_tuples = []
spout_line = 1
sink_tuples = {}
sink_line = 1
minute = 1

# Open the output CSV file in write mode to start with a clean file
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    fieldnames = ["minute", "latency_1", "throughput_1", "latency_2", "throughput_2", "latency_3", "throughput_3"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    while True:
        start = timeit.default_timer()
        spout_tuples, spout_line = get_spout_tuples(SPOUT_FILE, spout_line)
        print(f"Minute {minute} - Spout tuples: {len(spout_tuples)}")
        if not spout_tuples:
            print("All data read! Now sleeping for the rest of the minute zzz...")
            writer.writerow({"minute": minute, "latency_1": 0, "throughput_1": 0, "latency_2": 0, "throughput_2": 0, "latency_3": 0, "throughput_3": 0})
            end = timeit.default_timer()
            exec_time = end - start
            print(f"\tExecution Time: {exec_time} seconds\n")
            minute += 1
            time.sleep(max(60 - exec_time, 0)) # Want to sleep for the rest of the current minute to allow more data to come in
            continue
        sink_tuples, sink_line = get_sink_tuples(SINK_FILE, sink_line, sink_tuples)
        print(f"Minute {minute} - Sink tuples: {len(sink_tuples)}")
        throughput, tail_latency = calculate_latency(spout_tuples, sink_tuples)
        print(f"Minute {minute} - Throughput: {throughput}, Tail Latency: {tail_latency}")

        writer.writerow({
            "minute": minute,
            "latency_1": tail_latency[1],
            "throughput_1": throughput[1],
            "latency_2": tail_latency[2],
            "throughput_2": throughput[2],
            "latency_3": tail_latency[3],
            "throughput_3": throughput[3]
        })

        print(f"\tWritten to CSV: minute={minute}, latency_1={tail_latency[1]}, throughput_1={throughput[1]}, latency_2={tail_latency[2]}, throughput_2={throughput[2]}, latency_3={tail_latency[3]}, throughput_3={throughput[3]}")
        print(f"\tThroughput: {throughput} tuples")
        print(f"\t95th Percentile Tail Latency: {tail_latency} ms")

        end = timeit.default_timer()
        exec_time = end - start
        print(f"\tExecution Time: {exec_time} seconds")
        minute += 1
        print("Sleeping for the rest of the minute zzz...\n")
        time.sleep(max(60 - exec_time, 0)) # Want to sleep for the rest of the current minute to allow more data to come in