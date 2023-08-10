package in.dream_lab.bm.stream_iot.storm.spouts;

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
	Values[] values3;
	Values[] values2;
	Values[] values1;

	@Override
	public void nextTuple() 
	{

		int i=-1, j=-1, k=-1;
		int count = 0, MAX_COUNT=10; // FIXME?
		while(count < MAX_COUNT) 
		{
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) return;
			count++;
			msgId++;
			try 
			{
				ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId);
				//jr.batchWriter(System.currentTimeMillis(),"MSGID_" + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}
			String rowString = rowStringBuf.toString().substring(1);
			String newRow = rowString.substring(rowString.indexOf(",")+1);
			int a = Integer.parseInt(priority[p1]);
			p1++;
			if(a==3){
				i++;
				values3[i].add(Long.toString(msgId));
				values3[i].add(newRow);
			}
			if(a=='2'){
				j++;
				values2[j].add(Long.toString(msgId));
				values2[j].add(newRow);
			}
			if(a=='1'){
				k++;
				values1[k].add(Long.toString(msgId));
				values1[k].add(newRow);
			}
		}
		while (i!=-1){
			this._collector.emit(values3[i]);
			i--;
		}
		while(j!=-1){
			this._collector.emit(values2[j]);
			j--;
		}
		while(k!=-1){
			this._collector.emit(values1[k]);
			k--;
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

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());
		//jr=new JRedis(this.outSpoutCSVLogFileName);
		try 
		{
			FileReader reader = new FileReader("/home/amna/riot-bench-master/modules/tasks/src/main/resources/priority_sys.csv");
			BufferedReader br = new BufferedReader(reader);
			while ((br.readLine()) != null)   //returns a Boolean value  
			{  
				line = br.readLine();
				System.out.printf("data "+ line);
				priority[p]=line;
				p++;
			} 
		}   
		catch(Exception e)
	 	{
		  
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

