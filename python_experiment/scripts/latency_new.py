import sys
import time
import timeit
import linecache

# Usgage: python latency_new.py SPOUT_FILE SINK_FILE
# Spout Tuple: [time, "MSGID", pid, priority]
# Sink Tuple: [time, pid]
HOME = "/home/cc"
SPOUT_FILE = f"{HOME}/storm/riot-bench/output/{sys.argv[1]}"
SINK_FILE = f"{HOME}/storm/riot-bench/output/{sys.argv[2]}"
OUTPUT_FILE = f"{HOME}/storm/riot-bench/output/output-{sys.argv[2][5:]}"
print(f"Spout File: {SPOUT_FILE}")
print(f"Sink File: {SINK_FILE}")
print(f"Program Output File: {OUTPUT_FILE}")

# Algorithm Idea
# Use hash table of sink data for much faster O(n) searching - overwhelming majority of processes will exist in the hash table
# If a process does not exist in the hash table - use slower O(n^2) exhaustive search algorithm to find it instead
# If it is still not found at this point, then the process was dropped and did not reach the sink

# Given spout file stream and starting line number, get 1 minute's worth of tuples
def get_spout_tuples(spout_file, spout_line):
    spout_tuples = []
    tuple = linecache.getline(spout_file, spout_line).strip()
    # For each new tuple, check time and collect it if it's within 1 minute
    while tuple != "":
        time, msgid, pid, priority = tuple.split(",")
        time = int(time)
        pid = int(pid)
        if len(spout_tuples) == 0:
            start_time = time
        if time <= start_time + 60000: # Not doing start_time <= time as some data are delayed slightly
            spout_tuples.append([time, pid])
            spout_line += 1
            tuple = linecache.getline(spout_file, spout_line).strip()
        else:
            break
    return spout_tuples, spout_line

# Given sink file stream and starting line number, get/update hash table of sink tuples sorted by pid number
def get_sink_tuples(sink_file, sink_line, sink_tuples):
    tuple = linecache.getline(sink_file, sink_line).strip()
    # For each new tuple in the file, add it to the list
    while tuple != "":
        time, pid = map(int, tuple.split(","))
        sink_tuples.append([time, pid])
        sink_line += 1
        tuple = linecache.getline(sink_file, sink_line).strip()
    sink_tuples = sorted(sink_tuples, key = lambda x : x[1])
    return sink_tuples, sink_line


# Given 1 minute's worth of tuples and sink tuple hash table, calculate throughput and 95th percentile tail latency
def calculate_latency(spout_tuples, sink_tuples):
    latencies = []
    # For each collected tuple, find its end time (by accessing the hash table via its pid) and calculate latency
    for spout_tuple in spout_tuples:
        min_pid = sink_tuples[0][1]
        time_spout, pid_spout = spout_tuple
        try:
            time_sink, pid_sink = sink_tuples[pid_spout - min_pid]
            # time_sink = -1
            # pid_sink = -1
        except: # Index out of bounds (a packet was dropped, or pids are not consecutive) - use slower exhaustive search algorithm instead
            print("This should never happen on COMPLETE, FINISHED data - 1")
            #time_sink = -1
            #pid_sink = -1

        # If the hash table entry does not match or the index is out of bounds, then at least one packet was dropped
        # Will need to search through all of sink_tuples now - use slower exhaustive search algorithm insead
        # If still not found, then this is a packet that was dropped
        if pid_spout != pid_sink:
            print("This should never happen on COMPLETE, FINISHED data - 2")
            #print(f"NOTE: Process ID {pid_spout} was not found at the expected index in the hash table!")
            #found = False
            #for sink_tuple in sink_tuples:
                #time_sink, pid_sink = sink_tuple
                #if pid_spout == pid_sink:
                    #latencies.append(time_sink - time_spout)
                    #found = True
                    #break
            #if found == False:
                #print(f"NOTE: Could not find process ID {pid_spout} in the sink!")
        else:
            latencies.append(time_sink - time_spout)
        del sink_tuples[pid_spout - min_pid]

    # Calculate throughput and 95th percentile tail latency
    if len(latencies) == 0:
        return 0, 0
    else:
        latencies = sorted(latencies)
        throughput = len(latencies)
        tail_latency = latencies[int(len(latencies) * 0.95)]
        return throughput, tail_latency

print("Sleeping for 1 minute to allow data to come in zzz...") # Data starts being transmitted 60 to 90 seconds after topology start
time.sleep(60)
spout_tuples = []
spout_line = 1
sink_tuples = []
sink_line = 1
minute = 1
while True:
    start = timeit.default_timer()
    with open(OUTPUT_FILE, "a") as output:
        spout_tuples, spout_line = get_spout_tuples(SPOUT_FILE, spout_line)
        print(f"Minute {minute}")
        if (spout_tuples == []):
            print("All data read! Now sleeping for the rest of the minute zzz...")
            end = timeit.default_timer()
            exec_time = end - start
            print(f"\tExecution Time: {exec_time} seconds\n")
            minute += 1
            time.sleep(60 - exec_time) # Want to sleep for the rest of the current minute to allow more data to come in
            continue
        sink_tuples, sink_line = get_sink_tuples(SINK_FILE, sink_line, sink_tuples)
        throughput, tail_latency = calculate_latency(spout_tuples, sink_tuples)
        output.write(f"{minute} {throughput} {tail_latency}\n")
        print(f"\tThroughput: {throughput} tuples")
        print(f"\t95th Percentile Tail Latency: {tail_latency} ms")
    end = timeit.default_timer() 
    exec_time = end - start
    print(f"\tExecution Time: {exec_time} seconds")
    minute += 1
    print("Sleeping for the rest of the minute zzz...\n")
    time.sleep(max(60 - exec_time, 0)) # Want to sleep for the rest of the current minute to allow more data to come in