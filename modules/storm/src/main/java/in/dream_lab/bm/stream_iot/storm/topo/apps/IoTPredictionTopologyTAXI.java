package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */


//import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.*;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.AzureBlobDownloadTaskBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.MQTTSubscribeSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.io.File;


/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTPredictionTopologyTAXI {

    public static void main(String[] args) throws Exception {

        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        String taskPropFilename=argumentClass.getTasksPropertiesFilename();
        HashMap<String, String> topologyMap = argumentClass.getTopologyMap();
        System.out.println("taskPropFilename-"+taskPropFilename);


        Config conf = new Config();
        conf.setDebug(false);
        conf.put("topology.backpressure.enable",true);
        //conf.setNumWorkers(12);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);



        TopologyBuilder builder = new TopologyBuilder();


        String basePathForMultipleSpout=argumentClass.getInputDatasetPathName();

        //String spout1InputFilePath="/Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/TAXI-inputcsv-predict-10spouts200mps-480sec-file/";

        //System.out.println("basePathForMultipleSpout is used -"+basePathForMultipleSpout);
        //String spout1InputFilePath=basePathForMultipleSpout+"TAXI_sample_data_senml.csv";

        File f = new File(taskPropFilename);
        String spout1InputFilePath = f.getParentFile()+"/" + "TAXI_sample_data_senml.csv";


        //String spout1InputFilePath="/home/cc/storm/riot-bench/modules/tasks/src/main/resources/TAXI_sample_data_senml.csv";
//        String spout1InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-predict-10spouts200mps-480sec-file1.csv";
        String spout2InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-predict-10spouts200mps-480sec-file2.csv";


        builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1).addConfiguration("tags", topologyMap.get("spout1"));
//        builder.setSpout("spout2", new SampleSenMLSpout(spout2InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);

        builder.setBolt("SenMLParseBoltPREDTAXI",
                new SenMLParseBoltPREDTAXI(p_), 1)
                .shuffleGrouping("spout1").addConfiguration("tags", topologyMap.get("SenMLParseBoltPRED"));

/*
        builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1).addConfiguration("tags", topologyMap.get("spout1"));
        builder.setBolt("SenMLParseBoltPREDTAXI1",
                new SenMLParseBoltPREDTAXI(p_), 1)
                .shuffleGrouping("spout1").addConfiguration("tags", topologyMap.get("SenMLParseBoltPRED1"));
*/

//        builder.setSpout("mqttSubscribeTaskBolt",
//                new MQTTSubscribeSpout(p_,"dummyLog"), 1); // "RowString" should have path of blob

        //builder.setBolt("AzureBlobDownloadTaskBolt",
       //         new AzureBlobDownloadTaskBolt(p_), 1)
      //          .shuffleGrouping("mqttSubscribeTaskBolt");

        builder.setBolt("DecisionTreeClassifyBolt",
                new in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI.DecisionTreeClassifyBolt(p_), 1)
//		.shuffleGrouping("SenMLParseBoltPREDTAXI1")
                .shuffleGrouping("SenMLParseBoltPREDTAXI").addConfiguration("tags", topologyMap.get("DecisionTreeClassifyBolt"));
        //        .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));

        builder.setBolt("LinearRegressionPredictorBolt",
                new in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.TAXI.LinearRegressionPredictorBolt(p_), 1)
 //		.shuffleGrouping("SenMLParseBoltPREDTAXI1")
         	.shuffleGrouping("SenMLParseBoltPREDTAXI").addConfiguration("tags", topologyMap.get("LinearRegressionPredictorBolt"));
        //        .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));

        builder.setBolt("BlockWindowAverageBolt",
                new BlockWindowAverageBolt(p_), 1)
//        	.shuffleGrouping("SenMLParseBoltPREDTAXI1")
                .shuffleGrouping("SenMLParseBoltPREDTAXI").addConfiguration("tags", topologyMap.get("BlockWindowAverageBolt"));

        builder.setBolt("ErrorEstimationBolt",
                new ErrorEstimationBolt(p_), 1)
                .shuffleGrouping("BlockWindowAverageBolt")
                .shuffleGrouping("LinearRegressionPredictorBolt").addConfiguration("tags", topologyMap.get("ErrorEstimationBolt"));

        builder.setBolt("MQTTPublishBolt",
                new MQTTPublishBolt(p_), 1)
                .fieldsGrouping("ErrorEstimationBolt",new Fields("ANALAYTICTYPE"))
                .fieldsGrouping("DecisionTreeClassifyBolt",new Fields("ANALAYTICTYPE")).addConfiguration("tags", topologyMap.get("MQTTPublishBolt"));

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("MQTTPublishBolt").addConfiguration("tags", topologyMap.get("sink"));


        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(1000000000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
    }
}


//    L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     SYS-210  0.001   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

//    L   IdentityTopology  /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/TAXI-inputcsv-predict-10spouts200mps-480sec-file/TAXI-inputcsv-predict-10spouts200mps-480sec-file1.csv    SYS-210  0.001   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

//    L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     TAXI-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_TAXI.properties  test
