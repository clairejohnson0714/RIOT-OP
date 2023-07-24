package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.TAXI;

import in.dream_lab.bm.stream_iot.storm.genevents.factory.ReadFile;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskSYS;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskTAXI;
import org.apache.storm.shade.com.google.common.base.Stopwatch;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AzureTableRangeQueryBolt extends BaseRichBolt {

    Properties p; String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public AzureTableRangeQueryBolt(Properties p_){
        this.csvFileNameOutSink = csvFileNameOutSink; p=p_;

    }
    OutputCollector collector;

    AzureTableRangeQueryTaskTAXI azureTableRangeQueryTaskTAXI;


    private static Logger l; // TODO: Ensure logger is initialized before use
    public static void initLogger(Logger l_) {
        l = l_;
    }
    String ROWKEYSTART;
    String ROWKEYEND;


    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        azureTableRangeQueryTaskTAXI = new AzureTableRangeQueryTaskTAXI();


        initLogger(LoggerFactory.getLogger("APP"));

        azureTableRangeQueryTaskTAXI.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {

        // path for both model files
//        String BlobModelPath = input.getStringByField("BlobModelPath");
//        String analyticsType = input.getStringByField("analyticsType");

        String msgId = input.getStringByField("MSGID");
        ROWKEYSTART= (input.getStringByField("ROWKEYSTART"));
        ROWKEYEND= (input.getStringByField("ROWKEYEND"));


        if(l.isInfoEnabled())
            l.info("ROWKEYSTART:{} ROWKEYEND{}",ROWKEYSTART,ROWKEYEND);

        HashMap<String, String> map = new HashMap();
        map.put("ROWKEYSTART", ROWKEYSTART);
        map.put("ROWKEYEND", ROWKEYEND);

//        Stopwatch stopwatch=null;
//        if(l.isInfoEnabled()) {
//            stopwatch = Stopwatch.createStarted(); //
//        }

        //azureTableRangeQueryTaskTAXI.doTask(map);

        //Iterable<AzureTableRangeQueryTaskTAXI.Taxi_Trip> result= (Iterable<AzureTableRangeQueryTaskTAXI.Taxi_Trip>) azureTableRangeQueryTaskTAXI.getLastResult();

        //StringBuffer bf = new StringBuffer();
        // Loop through the results, displaying information about the entity
	    //String temp = null;
        //for (AzureTableRangeQueryTaskTAXI.Taxi_Trip entity : result) {
        //  if(l.isInfoEnabled())
        //      l.info("partition key {} and fareamount{}",entity.getPartitionKey(),entity.getFare_amount());
        //    bf.append(entity.getTrip_time_in_secs()).append(",")
        //        .append(entity.getTrip_distance()).append(",")
        //        .append(entity.getFare_amount()).append("\n");
		//    temp = entity.getTrip_distance();
        //}
        //        if(l.isInfoEnabled()) {
        //            stopwatch.stop(); // optional
        //            l.info("Time elapsed for azureTableRangeQueryTask() is {}", stopwatch.elapsed(MILLISECONDS)); //
        //        }
        if(l.isInfoEnabled())
            //l.info("data for annotation {}",bf.toString());
	        l.info("data for annotation");

        String inputFileName = "/home/cc/storm/riot-bench/modules/tasks/src/main/resources/TAXI_sample_data_senml.csv";
        ReadFile readFile = new ReadFile(inputFileName, ROWKEYSTART, ROWKEYEND);
        StringBuffer bf = readFile.readDataTaxi();

        //FIXME: read and emit model for DTC
	    if(bf.length() > 0)
             collector.emit(new Values(bf.toString(), msgId, ROWKEYEND));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("TRAINDATA","MSGID","ROWKEYEND"));
    }

}
