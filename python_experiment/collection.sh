cmd="bash /home/cc/measure/start.sh"
ssh raspberry3B $cmd
ssh raspberry3B1 $cmd
ssh worker1 $cmd

app="IoTPredictionTopology"
if [[ "$1" =~ "ETL" ]]; then
app="ETLTopologySYS"
fi

echo "start collecting"
for i in {1..5}
do
sleep 120
python perf.py $app &
echo $i
done

echo "stop collection"
cmd="ps aux | grep perf_info.sh | grep -v grep | awk '{print \$2}' | xargs kill"
ssh -tt raspberry3B1 $cmd
ssh -tt raspberry3B $cmd
ssh -tt worker1 $cmd

echo "end collecting"
