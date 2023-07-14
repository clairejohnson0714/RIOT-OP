home_path="/home/cc/"
home_source=${home_path}"storm/riot-bench/modules/tasks/src/main/resources/"
app_path="${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar"
output_path="${home_path}storm/riot-bench/output/"


data_type="TAXI"
app_name="IoTTrainTopology"$data_type
input_name="${home_source}inputFileForTimerSpout-TAXI.csv"
${home_path}storm/bin/storm kill ${app_name}
scale="20"

cd ~/storm/riot-bench/

~/maven/bin/mvn clean compile package -DskipTests

cd -

#sleep 40

${home_path}storm/bin/storm jar $app_path in.dream_lab.bm.stream_iot.storm.topo.apps.${app_name} C ${app_name} $input_name PLUG-210 $scale $output_path ${home_source}tasks_TAXI.properties test "spout1:core1"

#  Command Meaning: topology-fully-qualified-name <local-or-cluster> <Topo-name> <input-dataset-path-name> <Experi-Run-id> <scaling-factor> 
#<output dir name> <tasks properites filename> <tasks name>

# <task name> only uses in micro. 
#    Example command: SampleTopology L NA /var/tmp/bangalore.csv E01-01 0.001
