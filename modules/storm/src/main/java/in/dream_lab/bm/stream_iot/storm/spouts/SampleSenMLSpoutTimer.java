package in.dream_lab.bm.stream_iot.storm.spouts;

import in.dream_lab.bm.stream_iot.storm.genevents.EventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.ISyntheticEventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;

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

public class SampleSenMLSpoutTimer extends BaseRichSpout implements ISyntheticEventGen {
       static long timerWindowinMilliSec=60000;
       long startTimer=0;
       static long timerWindowinMilliSec2=40;
       long startTimer2=0;
 
       long currentTime;
       int latencyIndex = 0;
       private static final int[] latencyArray = {40, 36, 32, 29, 26, 23, 20, 18,16, 14, 12, 10, 8,7, 6,5, 4,3, 2,1,
	0, 0, 0, 0, 0, 0, 0, 0, 0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 2, 3,4,5, 6,7, 8, 10, 12, 14, 16, 18, 20,23, 26, 29, 32, 36, 40};

       private static final int[] latencyArray30min = {40, 32, 29, 20, 12, 8, 4,3, 2,1,
	0, 0, 0, 0, 0, 0, 0, 0, 0,0, 
        1, 2, 3,4, 8, 12, 20, 29, 32, 40};
	
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;
	public SampleSenMLSpoutTimer(){
		//			this.csvFileName = "/home/ubuntu/sample100_sense.csv";
		//			System.out.println("Inside  sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
		//			System.out.print("the output is as follows");
	}

	public SampleSenMLSpoutTimer(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public SampleSenMLSpoutTimer(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}

	@Override
	public void nextTuple() 
	{

        currentTime=System.currentTimeMillis();
	// every 1 minute to change the input rate.
        if((currentTime-startTimer)>timerWindowinMilliSec)
        {
            startTimer=currentTime;
	    latencyIndex += 1;
            if(latencyIndex == 30)latencyIndex = 0;
//            timerWindowinMilliSec2 = latencyArray[latencyIndex];
             timerWindowinMilliSec2 = 0;
 
       }

	//used to control input rate
        if((currentTime-startTimer2)>=timerWindowinMilliSec2)
        {
 
		int count = 0, MAX_COUNT=10; // FIXME? Orignal is 10.
		while(count < MAX_COUNT) 
		{
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) return;
			count++;
			Values values = new Values();
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}
//                       System.out.println("ROWKEYSTART:"+rowStringBuf.toString());


			String rowString = rowStringBuf.toString().substring(1);
			String newRow = rowString.substring(rowString.indexOf(",")+1);
			msgId++;
			values.add(Long.toString(msgId));
			values.add(newRow);
			this._collector.emit(values);
			try 
			{
				ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	  
        	startTimer2=currentTime;
        }else{
		int count = 0, MAX_COUNT=10; // FIXME?
		while(count < MAX_COUNT) 
		{
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) return;
			count++;
			Values values = new Values();
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}

			String rowString = rowStringBuf.toString().substring(1);
			String newRow = rowString.substring(rowString.indexOf(",")+1);
			msgId++;
			values.add(Long.toString(msgId));
			values.add(newRow);
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
        
                startTimer=System.currentTimeMillis();
                startTimer2=startTimer;
 
		this.eventGen = new EventGen(this,this.scalingFactor);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
		this.eventGen.launch(this.csvFileName, uLogfilename, -1, true); //Launch threads

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());


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

