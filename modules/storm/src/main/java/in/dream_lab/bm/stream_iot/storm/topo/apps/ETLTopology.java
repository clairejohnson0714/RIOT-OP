package in.dream_lab.bm.stream_iot.storm.topo.apps;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

//import org.apache.storm.generated.Nimbus.Iface;
//import org.apache.storm.generated.KillOptions;
//import org.apache.storm.generated.Nimbus;
//import org.apache.storm.utils.NimbusClient;


import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.AnnotationBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.AzureTableInsertBolt;
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

public class ETLTopology 
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

		 HashMap<String, String> topologyMap = argumentClass.getTopologyMap();

		 Config conf = new Config();
		 conf.setDebug(false);
	         conf.put("topology.backpressure.enable", true);
 	
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
//
	
		int bolts_num=1;
		int tasks_num=4;
        builder.setSpout("spout1",
				new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, 
argumentClass.getScalingFactor()
),
				1).addConfiguration("tags", topologyMap.get("spout1"));
//       builder.setSpout("spout10", new SampleSenMLSpout(spout10InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
		 
	   builder.setBolt("SenMlParseBolt",
	                new SenMLParseBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .shuffleGrouping("spout1").addConfiguration("tags", topologyMap.get("SenMlParseBolt"));//.setMemoryLoad(32);

    
//         			.shuffleGrouping("spout4")
//         			.shuffleGrouping("spout5")
        builder.setBolt("RangeFilterBolt",
	                new RangeFilterBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("SenMlParseBolt", new Fields("OBSTYPE")).addConfiguration("tags", topologyMap.get("RangeFilterBolt"));


	builder.setBolt("BloomFilterBolt",
	                new BloomFilterCheckBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("RangeFilterBolt", new Fields("OBSTYPE")).addConfiguration("tags", topologyMap.get("BloomFilterBolt"));
/*
       builder.setSpout("spout2",
				new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
				1).addConfiguration("tags", topologyMap.get("spout2"));
       builder.setBolt("SenMlParseBolt1",
	                new SenMLParseBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .shuffleGrouping("spout2").addConfiguration("tags", topologyMap.get("SenMlParseBolt1"));//.setMemoryLoad(32);
        builder.setBolt("RangeFilterBolt1",
	                new RangeFilterBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("SenMlParseBolt1", new Fields("OBSTYPE")).addConfiguration("tags", topologyMap.get("RangeFilterBolt1"));
	builder.setBolt("BloomFilterBolt1",
	                new BloomFilterCheckBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("RangeFilterBolt1", new Fields("OBSTYPE")).addConfiguration("tags", topologyMap.get("BloomFilterBolt1"));
        builder.setBolt("InterpolationBolt1",
	                new InterpolationBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("BloomFilterBolt1", new Fields("OBSTYPE")).addConfiguration("tags", topologyMap.get("InterpolationBolt1"));
	builder.setBolt("JoinBolt1",
	                new JoinBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	         	.fieldsGrouping("InterpolationBolt1", new Fields("MSGID"))
			.addConfiguration("tags", topologyMap.get("JoinBolt1"));//.setMemoryLoad(256);
	builder.setBolt("AnnotationBolt1",
	                new AnnotationBolt(p_), 1)
			.shuffleGrouping("JoinBolt1").addConfiguration("tags", topologyMap.get("AnnotationBolt1"));
	
	builder.setBolt("CsvToSenMLBolt1",
	                new CsvToSenMLBolt(p_), 1)
	                .shuffleGrouping("AnnotationBolt1").addConfiguration("tags", topologyMap.get("CsvToSenMLBolt1"));
 
	builder.setBolt("PublishBolt1",
	                new MQTTPublishBolt(p_), 1)
	                .shuffleGrouping("CsvToSenMLBolt1").addConfiguration("tags", topologyMap.get("PublishBolt1"));

*/	 
	builder.setBolt("InterpolationBolt",
	                new InterpolationBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("BloomFilterBolt", new Fields("OBSTYPE")).addConfiguration("tags", topologyMap.get("InterpolationBolt"));
	builder.setBolt("JoinBolt",
	                new JoinBolt(p_), bolts_num)
                        .setNumTasks(tasks_num)
	                .fieldsGrouping("InterpolationBolt", new Fields("MSGID")).addConfiguration("tags", topologyMap.get("JoinBolt"));
	builder.setBolt("AnnotationBolt",
	                new AnnotationBolt(p_), 1)
			.shuffleGrouping("JoinBolt").addConfiguration("tags", topologyMap.get("AnnotationBolt"));
		 
	 
	builder.setBolt("CsvToSenMLBolt",
	                new CsvToSenMLBolt(p_), 1)
	                .shuffleGrouping("AnnotationBolt").addConfiguration("tags", topologyMap.get("CsvToSenMLBolt"));
 
	builder.setBolt("PublishBolt",
	                new MQTTPublishBolt(p_), 1)
	                .shuffleGrouping("CsvToSenMLBolt").addConfiguration("tags", topologyMap.get("PublishBolt"));
 	builder.setBolt("sink", new Sink(sinkLogFileName), 1)
//				.shuffleGrouping("PublishBolt1")
         			.shuffleGrouping("PublishBolt").addConfiguration("tags", topologyMap.get("sink"));
//		            .shuffleGrouping("AzureInsert");
		 
		 StormTopology stormTopology = builder.createTopology();
		 
		 if (argumentClass.getDeploymentMode().equals("C")) 
		 {
	            	StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology); 	

        		// create Nimbus client
        		//NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        		// create KillOptions object
        		//KillOptions killOpts = new KillOptions();
                        //Utils.sleep(900000);
			//killOpts.set_wait_secs(0);
			//Iface client = nimbusClient.getClient();
        		// kill topology
        		//client.killTopologyWithOpts(argumentClass.getTopoName(), killOpts);
	        } else {
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
	            Utils.sleep(900000);
	            cluster.killTopology(argumentClass.getTopoName());
	            cluster.shutdown();
	        }
	 }
}
