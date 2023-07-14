package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */


import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SenMLParseBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.MQTTSubscribeSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
//import in.dream_lab.bm.stream_iot.storm.spouts.TimeSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.io.File;

/**
 * Created by anshushukla on 18/05/15.
 */
public class ETLTopologySimplify2{

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
        System.out.println("taskPropFilename-"+taskPropFilename);

	
        Config conf = new Config();
        conf.setDebug(false);
        conf.put("topology.backpressure.enable", false);
        conf.setNumWorkers(2);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);



        TopologyBuilder builder = new TopologyBuilder();


//        String basePathForMultipleSpout="/Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsv-10spouts600mps-480sec-file/";
        String basePathForMultipleSpout="/home/cc/storm/riot-bench/modules/tasks/src/main/resources/";

//        String basePathForMultipleSpout="/home/anshu/data/storm/dataset/SYS-splitted-data/";

        System.out.println("basePathForMultipleSpout is used -"+basePathForMultipleSpout);

        //String spout1InputFilePath=basePathForMultipleSpout+"SYS_sample_data_senml.csv";
 	File f = new File(taskPropFilename);
	String spout1InputFilePath = f.getParentFile()+"/" + "SYS_sample_data_senml.csv";
      
//        String spout1InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file1.csv";
        builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1).addConfiguration("tags", "edge1").setMemoryLoad(128);
//        builder.setSpout("spout2", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()), 1).addConfiguration("tags", "edge2").setMemoryLoad(128);
        
        builder.setBolt("SenMLParseBoltPREDSYS",
                new SenMLParseBoltPREDSYS(p_), 1)
                .shuffleGrouping("spout1").addConfiguration("tags", "core1").setMemoryLoad(256);
//                 	.shuffleGrouping("spout2")
        ;

        builder.setBolt("LinearRegressionPredictorBolt",
                new LinearRegressionPredictorBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDSYS").addConfiguration("tags", "core1").setMemoryLoad(128);

        builder.setBolt("ErrorEstimationBolt",
                new ErrorEstimationBolt(p_), 1)
                .shuffleGrouping("LinearRegressionPredictorBolt").addConfiguration("tags", "edge1").setMemoryLoad(128);

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("ErrorEstimationBolt").addConfiguration("tags", "core1").setMemoryLoad(128);

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
