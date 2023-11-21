# Create the 'data' directory if it doesn't exist
mkdir -p data

# Store the process ID (PID) of the data collection script
data_collection_pid=""

# Function to start the data collection script in the background
start_data_collection() {
    python3 perf_priority_taxi.py &
    data_collection_pid=$!

    # Add a loop to flush Redis every 4 minutes (240 seconds)
    #while [ -n "$data_collection_pid" ]; do
        #sleep 240  # Sleep for 4 minutes
        #redis-cli flushall  # Clear old data from Redis
    #done
}

# Function to stop the data collection script
stop_data_collection() {
    if [ -n "$data_collection_pid" ]; then
        kill "$data_collection_pid"
        data_collection_pid=""
    fi
}

# Function to handle script cleanup on exit
cleanup() {
    stop_data_collection
    # Add any other cleanup steps here
}

# Register the cleanup function to be called when the script exits
trap cleanup EXIT

# Loop from 1 to 20
for i in {9..11}
do
    # Clear old data
    redis-cli flushall

    # Copy 'p1.txt' to the application directory
    cp /home/cc/storm/riot-bench/python_experiment/Priority/p$i.txt /home/cc/storm/riot-bench/modules/tasks/src/main/resources/priority_sys.txt

    # Start the application in the background
    bash /home/cc/storm/riot-bench/python_experiment/scripts/run_PREDICT_taxi.sh 0.01 test &

    # Start the data collection script
    start_data_collection

    # Sleep for 1200 seconds (20 minutes) to run the application for testing just write 3-minute data 
    sleep 1200

    # Stop the data collection script
    stop_data_collection

    # Create a directory for the results and move them there
    result_dir="data/priority$i"
    mkdir -p "$result_dir"
    mv /home/cc/storm/riot-bench/output/skopt_input_IoTPredictionTopologyTAXI.csv "$result_dir/"

    # Kill the Storm topology
    /home/cc/storm/bin/storm kill "IoTPredictionTopologyTAXI"

    # Sleep for 150 seconds (2.5 minutes)
    sleep 150
done
