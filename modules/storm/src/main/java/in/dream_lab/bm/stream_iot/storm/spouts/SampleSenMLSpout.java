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
	JRedis jr;
	long msgId;
	String line;
	int p1=0;
	int p=0;
	String priority[];

    	private static Logger l = LoggerFactory.getLogger("APP");

	public SampleSenMLSpout(){
		this.csvFileName = "/home/ubuntu/sample100_sense.csv";
		//			System.out.println("Inside  sample spout code");
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
	//Values values3[];
	//Values  values2[];
	//Values  values1[];


	@Override
	public void nextTuple() 
	{
		ArrayList<Values> values1 = new ArrayList<Values>();
		ArrayList<Values> values2 = new ArrayList<Values>();
		ArrayList<Values> values3 = new ArrayList<Values>();
		Values value ;
		int priorityval=0;
		int i=0;
		int count = 0, MAX_COUNT=1; // FIXME?  For MAX_COUNT 10,20 would produce muliple data at sink
		while(count < MAX_COUNT) 
		{
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) break;
			
			count++;
			msgId++;

			if (p1 == 999) 
				p1 = 0;		
			//try 
			//{
				//ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId, priority[p1]);
				//if (msgId % 20 == 0)
				   // jr.batchWriter(System.currentTimeMillis(),"MSGID_" + msgId,priority[p1]);
			//} catch (Exception e) {
				//e.printStackTrace();
			//}
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
				//l.warn("SSSS {}",s);
			}
			String rowString = rowStringBuf.toString().substring(1);
			String newRow = rowString.substring(rowString.indexOf(",")+1);
			
			//l.warn("Faizavalue {}",newRow);
			
            		try 
					{
            			// Parse JSON string
            			ObjectMapper objectMapper = new ObjectMapper();
            			JsonNode jsonNode = objectMapper.readTree(newRow);

            			// Extract priority value
            			JsonNode priorityNode = jsonNode.at("/e/8/v");
            			l.warn("priorityValue as String *************"+ priorityNode );
						priorityval=priorityNode.asInt();
						l.warn("priorityval as integer *************"+ priorityval );
            
            		} 
					catch (Exception e) {
            			e.printStackTrace();
        			}
			//int a = Integer.parseInt(priority[p1]);
			p1++;
			if(priorityval==3){
				value = new Values();
				value.add(Long.toString(msgId));
				value.add(newRow);
				//l.warn("values3 {}",value);
				values3.add(value);
			}
			if(priorityval==2){
				value = new Values();
				value.add(Long.toString(msgId));
				value.add(newRow);
				//l.warn("values2 {}",value);
				values2.add(value);
			}
			if(priorityval==1){
				value = new Values();
				value.add(Long.toString(msgId));
				value.add(newRow);
				//l.warn("values1 {}",value);
				values1.add(value);
			}
		}
		for (i = 0; i < values3.size(); i++) {
			this._collector.emit(values3.get(i));

			ObjectMapper objectMapper = new ObjectMapper();
			try {
				List<Object> tuple = values3.get(i);
				if (tuple.size() == 2 && tuple.get(1) instanceof String) {
					String jsonString = (String) tuple.get(1);
					JsonNode jsonNode = objectMapper.readTree(jsonString);
					//l.warn("JSON Structure: " + jsonNode);
					JsonNode priorityNode = jsonNode.at("/e/8/v");
					//l.warn("Priority Value as String: " + priorityNode);
					int priorityValue = priorityNode.asInt();
					//l.warn("Priority Value as Integer: " + priorityValue);
					ba.batchLogwriter(System.currentTimeMillis(), "MSGID," + msgId, String.valueOf(priorityValue));
					jr.batchWriter(System.currentTimeMillis(), "MSGID_" + msgId, String.valueOf(priorityValue));
				} else {
					l.error("Invalid tuple structure: " + tuple);
				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

			
		}
		for (i = 0; i < values2.size(); i++) {
            		this._collector.emit(values2.get(i));
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				List<Object> tuple = values2.get(i);
				if (tuple.size() == 2 && tuple.get(1) instanceof String) {
					String jsonString = (String) tuple.get(1);
					JsonNode jsonNode = objectMapper.readTree(jsonString);
					//l.warn("JSON Structure: " + jsonNode);
					JsonNode priorityNode = jsonNode.at("/e/8/v");
					//l.warn("Priority Value as String: " + priorityNode);
					int priorityValue = priorityNode.asInt();
					//l.warn("Priority Value as Integer: " + priorityValue);
					ba.batchLogwriter(System.currentTimeMillis(), "MSGID," + msgId, String.valueOf(priorityValue));
					jr.batchWriter(System.currentTimeMillis(), "MSGID_" + msgId, String.valueOf(priorityValue));
				} else {
					l.error("Invalid tuple structure: " + tuple);
				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

        	}
		for (i = 0; i < values1.size(); i++) {
            		this._collector.emit(values1.get(i));
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				List<Object> tuple = values1.get(i);
				if (tuple.size() == 2 && tuple.get(1) instanceof String) {
					String jsonString = (String) tuple.get(1);
					JsonNode jsonNode = objectMapper.readTree(jsonString);
					//l.warn("JSON Structure: " + jsonNode);
					JsonNode priorityNode = jsonNode.at("/e/8/v");
					//l.warn("Priority Value as String: " + priorityNode);
					int priorityValue = priorityNode.asInt();
					//l.warn("Priority Value as Integer: " + priorityValue);
					ba.batchLogwriter(System.currentTimeMillis(), "MSGID," + msgId, String.valueOf(priorityValue));
					jr.batchWriter(System.currentTimeMillis(), "MSGID_" + msgId, String.valueOf(priorityValue));
				} else {
					l.error("Invalid tuple structure: " + tuple);
				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
			e.printStackTrace();
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
		
		long waitingToStart = System.currentTimeMillis() % 60000;
		try{
			Thread.sleep(waitingToStart);
		} catch ( Exception e) {
			e.printStackTrace();
		}

		this.eventGen.launch(this.csvFileName, uLogfilename, -1, true); //Launch threads

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());
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
				//l.warn("FaizaLine{}",line);
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

