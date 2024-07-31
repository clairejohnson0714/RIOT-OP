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
	long ts;
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
		//Dummy Arrays for time stamps
		ArrayList<Values> values11 = new ArrayList<Values>();
		ArrayList<Values> values22 = new ArrayList<Values>();
		ArrayList<Values> values33 = new ArrayList<Values>();
		Values value ;
		int priorityval=0;
		int i=0;
		int count = 0, MAX_COUNT=200; // FIXME?  For MAX_COUNT 10,20 would produce muliple data at sink
		
		while(count < MAX_COUNT) 
		{
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) break;
			
			count++;
			
			if (p1 == 999) 
				p1 = 0;		
			
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
				//l.warn("SSSS {}",s);
			}
			String rowString = rowStringBuf.toString().substring(1);
			String newRow = rowString.substring(rowString.indexOf(",")+1);
			//l.warn("Faizavalue {}",newRow);
			ts = System.currentTimeMillis();
			//l.warn("Time Before Extracting priorty"+ts);
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
			//int a = Integer.parseInt(priority[p1]);
			p1++;
			msgId++;
					
			//l.warn("MSG ID *************"+ msgId );
			if(priorityval==3){
				
				value = new Values();
				value.add(Long.toString(msgId));
				value.add(newRow);
				values3.add(value);
				
				value.add(ts);
				//l.warn("values33 {}",value);
				values33.add(value);
				//l.warn("values33 {}",values33);
			}
			if(priorityval==2){
				//ts = System.currentTimeMillis();
				value = new Values();
				value.add(Long.toString(msgId));
				value.add(newRow);
				//l.warn("values2 {}",value);
				values2.add(value);

				value.add(ts);
				//l.warn("values22 {}",value);
				values22.add(value);
			
			}
			if(priorityval==1){
				//ts =  System.currentTimeMillis();
				value = new Values();
				value.add(Long.toString(msgId));
				value.add(newRow);
				//l.warn("values1 {}",value);
				values1.add(value);

				value.add(ts);
				//l.warn("values11 {}",value);
				values11.add(value);
			}
		}
		// Modified New Method Logic for paper:FTS-Hybrid Cloud-Summit
		
		while (!values1.isEmpty() || !values2.isEmpty() || !values3.isEmpty())
        {
			int size1 = values1.size();
			int size2 = values2.size();
			int size3 = values3.size();

			int minSize = 0;

			// Find the minimum size of non-empty arrays
			if(size1==0 )
				size1=MAX_COUNT;
			if(size2==0)
				size2=MAX_COUNT;
			if(size3==0)
				size3=MAX_COUNT;
				
			minSize=Math.min(size1, Math.min(size2, size3));
			
			if (minSize > 0) {
				for (i = 0; i < minSize; i++) 
				{
			
					if (!values3.isEmpty()){
						this._collector.emit(values3.get(i));
						//l.warn("Emitted Tuple 3:"+values3.get(i));
						try {
							List<Object> element3  = (List<Object>) values33.get(i);
							if (element3.size() == 3 && element3.get(0) instanceof String && element3.get(2) instanceof Long ) {
								String values3_msgId = (String) element3.get(0);
								Long ts3 = (Long) element3.get(2);
								//ba.batchLogwriter(ts3, "MSGID," + values3_msgId, String.valueOf(3));
								jr.batchWriter(ts3, "MSGID_" + values3_msgId, String.valueOf(3));
							} 
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				for (i = 0; i < minSize; i++) 
				{
					if (!values2.isEmpty()){
						this._collector.emit(values2.get(i));
						//l.warn("Emitted Tuple 2:"+values2.get(i));
						try {			
							List<Object> element2 = values22.get(i);
							if (element2.size() == 3 && element2.get(0) instanceof String && element2.get(2) instanceof Long) {
								String values2_msgId = (String) element2.get(0);				
								Long ts2 = (Long) element2.get(2);
								//ba.batchLogwriter(ts2, "MSGID," + values2_msgId, String.valueOf(2));
								jr.batchWriter(ts2, "MSGID_" + values2_msgId, String.valueOf(2));
							} 
						} 
						catch (Exception e) {			
							e.printStackTrace();
						}
					}
				}
				for (i = 0; i < minSize; i++) 
				{
					if (!values1.isEmpty()){
					this._collector.emit(values1.get(i));
					//l.warn("Emitted Tuple 1:"+values1.get(i));
					try {					
						List<Object> element = values11.get(i);
							if (element.size() == 3 && element.get(0) instanceof String && element.get(2) instanceof Long) 
							{
								String values1_msgId = (String) element.get(0);					
								Long ts1 = (Long) element.get(2);
								//ba.batchLogwriter(ts1, "MSGID," + values1_msgId, String.valueOf(1));
								jr.batchWriter(ts1, "MSGID_" + values1_msgId, String.valueOf(1));
							}
					}
					catch (Exception e) {				
						e.printStackTrace();
					}
					}
				}	
		    }
            // Remove emitted elements from arrays
            if (!values1.isEmpty()) values1.subList(0, minSize).clear();
            if (!values2.isEmpty()) values2.subList(0, minSize).clear();
            if (!values3.isEmpty()) values3.subList(0, minSize).clear();
			if (!values11.isEmpty()) values11.subList(0, minSize).clear();
            if (!values22.isEmpty()) values22.subList(0, minSize).clear();
            if (!values33.isEmpty()) values33.subList(0, minSize).clear();
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
		
		//long waitingToStart = System.currentTimeMillis() % 60000;
		//try{
		//	Thread.sleep(waitingToStart);
		//} catch ( Exception e) {
		//	e.printStackTrace();
		//}

		this.eventGen.launch(this.csvFileName, uLogfilename, -1, true); //Launch threads

		//ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());
		jr=new JRedis(this.outSpoutCSVLogFileName);
 		

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

