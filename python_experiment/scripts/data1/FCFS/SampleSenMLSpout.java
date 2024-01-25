package in.dream_lab.bm.stream_iot.storm.spouts;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException; 

import in.dream_lab.bm.stream_iot.storm.genevents.EventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.ISyntheticEventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;
import in.dream_lab.bm.stream_iot.storm.genevents.logging.JRedis;
import java.io.BufferedReader;  
import java.io.FileReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileNotFoundException;

public class SampleSenMLSpout extends BaseRichSpout implements ISyntheticEventGen {
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;
	JRedis jr;
	int p1=0;
	int p=0;
	String priority[];
			private static Logger l = LoggerFactory.getLogger("APP");
	public SampleSenMLSpout(){
		//			this.csvFileName = "/home/ubuntu/sample100_sense.csv";
		//			System.out.println("Inside  sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
		//			System.out.print("the output is as follows");
	}

	public SampleSenMLSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public SampleSenMLSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}

	@Override
	public void nextTuple() 
	{
		
		int priorityval=0;
		int count = 0, MAX_COUNT=30; // FIXME?
		while(count < MAX_COUNT) 
		{
			Values values = new Values();
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) break;
			count++;

			if (p1 == 999) 
				p1 = 0;		
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}
			String rowString = rowStringBuf.toString().substring(1);
			String newRow = rowString.substring(rowString.indexOf(",")+1);
			//l.warn("newRow:"+newRow);
			long ts = System.currentTimeMillis();
			try 
				{
           			// Parse JSON string
           		 	ObjectMapper objectMapper = new ObjectMapper();
           		 	JsonNode jsonNode = objectMapper.readTree(newRow);

           		 	// Extract priority value
           		 	JsonNode priorityNode = jsonNode.at("/e/8/v");
           		 	//l.warn("priorityValue as String *************"+ priorityNode );
	   		     	priorityval=priorityNode.asInt();
	   		     	//l.warn("priorityval as integer *************"+ priorityval );
           		} 
				catch (Exception e) 
				{
            			e.printStackTrace();
        		}
			p1++;
			
			msgId++;
                        
		
			values.add(Long.toString(msgId));
			values.add(newRow);
			this._collector.emit(values);  //FCFS
			//l.warn("Emitted Tuple"+ values );
           		 
			try 
			{
				//ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId, String.valueOf(priorityval));
				//if (msgId % 20 == 0)
				jr.batchWriter(ts, "MSGID_" + msgId, String.valueOf(priorityval));
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) 
	{
		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);
		Random r=new Random();
		try 
		{
			msgId= (long) (1*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			
		} catch (Exception e) {

			e.printStackTrace();
		}
		_collector = collector;
		this.eventGen = new EventGen(this,this.scalingFactor);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
		this.eventGen.launch(this.csvFileName, uLogfilename, -1, true); //Launch threads

		//ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());
		jr=new JRedis(this.outSpoutCSVLogFileName);
 		priority = new String[1005];
		p = 0;
		try 
		{
		 	FileReader reader = new FileReader("/home/cc/storm/riot-bench/modules/tasks/src/main/resources/priority_sys.txt");
		 	BufferedReader br = new BufferedReader(reader);
		 	String line = br.readLine();
			while (line != null)   //returns a Boolean value  
			{  
				priority[p]=line.replace("\n", "");
				p++;
				line = br.readLine();
			} 
			br.close();

		}   
		catch(Exception e)
	 	{
			e.printStackTrace();
	  	}




	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
		declarer.declare(new Fields("MSGID" , "PAYLOAD"));
	}

	@Override
	public void receive(List<String> event) 
	{
		try 
		{
			this.eventQueue.put(event);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

