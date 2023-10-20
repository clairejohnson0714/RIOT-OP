package in.dream_lab.bm.stream_iot.storm.sinks;


import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import in.dream_lab.bm.stream_iot.storm.genevents.logging.JRedis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by anshushukla on 19/05/15.
 */
public class Sink extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger("APP");

    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    BatchedFileLogging ba;
    String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    JRedis jr;

    public Sink(String csvFileNameOutSink){
    	Random ran = new Random();
        this.csvFileNameOutSink = csvFileNameOutSink;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        BatchedFileLogging.writeToTemp(this, this.csvFileNameOutSink);
         //ba=new BatchedFileLogging();
        //ba=new BatchedFileLogging(this.csvFileNameOutSink, topologyContext.getThisComponentId());
        jr=new JRedis(this.csvFileNameOutSink);
    }

    @Override
    public void execute(Tuple input) {
        String msgId = input.getStringByField("MSGID");
        //String exe_time = input.getStringByField("time");  //addon
        //collector.emit(input,new Values(msgId));
        long ts = System.currentTimeMillis();
        try {
        	//ba.batchLogwriter(System.currentTimeMillis(),msgId);
           if (Long.parseLong(msgId) % 10 == 0)
	    	        jr.batchWriter(System.currentTimeMillis(), msgId);
		 if (Long.parseLong(msgId) % 20 == 0)
         		jr.batchWriter(ts, msgId);
            //ba.batchLogwriter(System.currentTimeMillis(),msgId+","+exe_time);//addon
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
