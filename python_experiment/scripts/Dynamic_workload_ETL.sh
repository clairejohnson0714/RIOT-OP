#!/bin/bash
# Create the 'data' directory if it doesn't exist
mkdir -p data1

# Function to start the data collection script in the background
start_data_collection() {
    python3 perf_priority_taxi_ETL.py &
    data_collection_pid=$!

    # Add a loop to flush Redis every 10-15 minutes for 2 hours
    #for ((i = 0; i < 8; i++)); do
        #redis-cli config set stop-writes-on-bgsave-error no
        #redis-cli flushall
        #sleep 120
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

# Loop for two applications
for app in "run_ETL_sys.sh" "run_ETL_taxi.sh"; do
    # Clear old data
    redis-cli -h redis flushall

    # Start the application in the background
    bash /home/cc/storm/riot-bench/python_experiment/scripts/"$app" 0.5 test

    # the starting second of every minute
    sleep $[ 60 - $(date +%s) % 60 + 2 ] 
    # Sleep for 1 hour (3600 seconds) to run the application
    for i in {1..60} ;
    	do
                echo $i
    		# Start the data collection script
    		start_data_collection

    		sleep 60

    		# Stop the data collection script
    		stop_data_collection
	done
    exit
    # Create a directory for the results and move them there
    app_name=$(echo "$app" | cut -d ' ' -f 1)
    result_dir="data1/$app_name"
    mkdir -p "$result_dir"
    mv "/home/cc/storm/riot-bench/output/skopt_input_ETLTopology$app_name.csv" "$result_dir/"

    # Kill the Storm topology
    /home/cc/storm/bin/storm kill "ETLTopologySYS"

    # Sleep for 150 seconds (2.5 minutes) before the next application
    sleep 150
done
