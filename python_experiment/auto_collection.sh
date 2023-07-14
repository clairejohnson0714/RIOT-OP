# compile application and start app
LOG="data/new_data.log"
rm $LOG

# 0.2 0.15 0.1 
inputRate=$1
# default, resource, amnis, matching, matchingPlus
scheduler=$2

# clean the old application
home_path="/home/cc/"
app="ETLTopologySYS"
app1="IoTPredictionTopologySYS"
app2="IoTPredictionTopologyTAXI"
app3="ETLTopologyTAXI"
${home_path}storm/bin/storm kill $app 
${home_path}storm/bin/storm kill $app1 
${home_path}storm/bin/storm kill $app2
${home_path}storm/bin/storm kill $app3
sleep 30


app1="run_PREDICT_sys.sh"
app2="run_ETL_sys.sh"
app3="run_PREDICT_taxi.sh"
app4="run_ETL_taxi.sh"


bash /home/cc/storm/riot-bench/python_experiment/scripts/$app1 $inputRate $scheduler
bash /home/cc/storm/riot-bench/python_experiment/scripts/$app2 $inputRate $scheduler
bash /home/cc/storm/riot-bench/python_experiment/scripts/$app3 $inputRate $scheduler
bash /home/cc/storm/riot-bench/python_experiment/scripts/$app4 $inputRate $scheduler

# clean redis data
redis-cli config set stop-writes-on-bgsave-error no
redis-cli flushall
sleep 120
#bash collection.sh $script >> $LOG &
bash collection2.sh >> ${LOG}2


# collect system information data
sleep 10

for hostname in "edge1" "edge2" "edge3" "edge4" "edge5" "worker2" "worker1" "core"
do
rm data/${hostname}Cpu.log
rm data/${hostname}Network.log
scp $hostname:/tmp/cpu.log data/${hostname}Cpu.log
scp $hostname:/tmp/network.log data/${hostname}Network.log
done

cd data
echo "input rate is $inputRate"
resultFile="read_data_${scheduler}.log"
python3 cal_data.py >> $resultFile
cat $resultFile
