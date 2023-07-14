app="ETLTopologySYS"
app1="IoTPredictionTopologySYS"
app2="IoTPredictionTopologyTAXI"
app3="ETLTopologyTAXI"

cmd="bash /home/cc/measure/start.sh"

for hostname in "core" "edge1" "edge2" "edge3" "edge4" "edge5" "worker2" "worker1"
do
ssh $hostname $cmd
done

LOG="data/"
rm $LOG$app
rm $LOG$app1
rm $LOG$app2
rm $LOG$app3
echo "start collecting"
for i in {1..6}
do
sleep 60
python perf.py $app >> $LOG$app &
python perf.py $app1 >> $LOG$app1 &
python perf.py $app2 >> $LOG$app2 &
python perf.py $app3 >> $LOG$app3 &

echo $i
done

echo "stop collection"
cmd="ps aux | grep perf_info.sh | grep -v grep | awk '{print \$2}' | xargs kill"

for hostname in "core" "edge1" "edge2" "edge3" "edge4" "edge5" "worker2" "worker1"
do
ssh $hostname $cmd
done 
