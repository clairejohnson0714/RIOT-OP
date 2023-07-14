// this file is add by Peng
package in.dream_lab.bm.stream_iot.storm.topo.apps;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.AnnotationBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.BloomFilterCheckBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.CsvToSenMLBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.InterpolationBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.JoinBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.MQTTPublishBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.RangeFilterBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SenMLParseBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.LinearRegressionPredictorBolt;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;

public class ETLTopologySimplify
{
	 public static void main(String[] args) throws Exception
	 {
		 ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
		 if (argumentClass == null) {
			 System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
			 return;
		 }
		 String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
		 String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
		 String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
		 String taskPropFilename=argumentClass.getTasksPropertiesFilename();
		 String spout1InputFilePath=argumentClass.getInputDatasetPathName();
		 
		 Config conf = new Config();
		 conf.setDebug(false);
		
		 conf.setNumWorkers(2);
		 
		 Properties p_=new Properties();
		 InputStream input = new FileInputStream(taskPropFilename);
		 p_.load(input);
		 TopologyBuilder builder = new TopologyBuilder();
		
		/*The below code shows how we can have multiple spouts read from different files 
		This is to provide multiple spout threads running at the same time but reading 
		data from separate file - Shilpa  */

//       String spout2InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file2.csv";
//       String spout3InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file3.csv";

	int bolts_num=1;
	int tasks_num=1;
        builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
               1).addConfiguration("tags", "core1");
//       builder.setSpout("spout2", new SampleSenMLSpout(spout2InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
		 
	   builder.setBolt("BSenMlParseBolt",
	                new SenMLParseBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .shuffleGrouping("spout1").addConfiguration("tags", "core1");//.setMemoryLoad(32);

        	//builder.setBolt("RangeFilterBolt",
	        //        new RangeFilterBolt(p_), bolts_num)
                //        .setNumTasks(tasks_num)
		//.fieldsGrouping("SenMlParseBolt", new Fields("OBSTYPE"));

		 //builder.setBolt("BloomFilterBolt",
	         //       new BloomFilterCheckBolt(p_), bolts_num)
                 //       .setNumTasks(tasks_num)
	         //       .fieldsGrouping("RangeFilterBolt", new Fields("OBSTYPE"));
		 
		 builder.setBolt("AInterpolationBolt",
	                new InterpolationBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("BSenMlParseBolt", new Fields("OBSTYPE")).addConfiguration("tags", "core1");
		 
		 builder.setBolt("JoinBolt",
	                new JoinBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("AInterpolationBolt", new Fields("MSGID")).addConfiguration("tags", "core1");
		 
		 //builder.setBolt("AnnotationBolt",
	         //       new AnnotationBolt(p_), 1)
	         //       .shuffleGrouping("JoinBolt"); 
		 
		 //builder.setBolt("CsvToSenMLBolt",
	         //       new CsvToSenMLBolt(p_), 1)
	         //       .shuffleGrouping("AnnotationBolt");
 
		 //builder.setBolt("PublishBolt",
	         //       new MQTTPublishBolt(p_), 1)
	         //       .shuffleGrouping("CsvToSenMLBolt");//.setMemoryLoad(32);

		 builder.setBolt("sink", new Sink(sinkLogFileName), 1)
         			.shuffleGrouping("JoinBolt").addConfiguration("tags", "edge1");
		 
		 StormTopology stormTopology = builder.createTopology();
		 
		 if (argumentClass.getDeploymentMode().equals("C")) 
		 {
	            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
	        } else {
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
	            Utils.sleep(900000);
	            cluster.killTopology(argumentClass.getTopoName());
	            cluster.shutdown();
	        }
	 }
}
