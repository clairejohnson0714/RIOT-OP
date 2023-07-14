home_path="/home/cc/"
home_source=${home_path}"storm/riot-bench/modules/tasks/src/main/resources/"
app_path="${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar"
output_path="${home_path}storm/riot-bench/output/"


#app_name="IoTStatsTopology"
app_name="Stats_SQL_Topology"
app_param="_TAXI"
data_type="TAXI"
scale=0.1
input_name="${home_source}${data_type}_sample_data_senml.csv"
${home_path}storm/bin/storm kill ${app_name}_$data_type

cd ~/storm/riot-bench/

#~/maven/bin/mvn clean compile package -DskipTests

cd -

#sleep 60

${home_path}storm/bin/storm jar $app_path in.dream_lab.bm.stream_iot.storm.topo.apps.${app_name} C ${app_name}_$data_type $input_name SENML-2100  $scale $output_path ${home_source}tasks_${data_type}.properties test

#  Command Meaning: topology-fully-qualified-name <local-or-cluster> <Topo-name> <input-dataset-path-name> <Experi-Run-id> <scaling-factor> 
#<output dir name> <tasks properites filename> <tasks name>

# <task name> only uses in micro. 
#    Example command: SampleTopology L NA /var/tmp/bangalore.csv E01-01 0.001
