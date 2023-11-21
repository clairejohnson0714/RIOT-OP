#!/bin/bash


# Stop ZooKeeper
/home/cc/zookeeper/bin/zkServer.sh stop

# Remove data files
rm -rf /home/cc/storm/data/*
rm -rf /home/cc/zookeeper/data/*

# List running processes related to Storm or Supervisor
ps aux | grep "storm\|supervisor"

# kill -9 process_id

# Start ZooKeeper
/home/cc/zookeeper/bin/zkServer.sh start &

# Start Storm services
/home/cc/storm/bin/storm nimbus &
/home/cc/storm/bin/storm supervisor &
/home/cc/storm/bin/storm ui &