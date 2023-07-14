package in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.SYS;

import in.dream_lab.bm.stream_iot.storm.genevents.factory.ReadFile;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskSYS;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AzureTableRangeQueryBolt extends BaseRichBolt {

    Properties p; String csvFileNameOutSink;  //Full path name of the file at the sink bolt
    public AzureTableRangeQueryBolt(Properties p_){
        this.csvFileNameOutSink = csvFileNameOutSink; p=p_;

    }
    OutputCollector collector;

    AzureTableRangeQueryTaskSYS azureTableRangeQueryTaskSYS;


    private static Logger l; // TODO: Ensure logger is initialized before use
    public static void initLogger(Logger l_) {
        l = l_;
    }
    String ROWKEYSTART;
    String ROWKEYEND;


    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));

        azureTableRangeQueryTaskSYS =new AzureTableRangeQueryTaskSYS();


        initLogger(LoggerFactory.getLogger("APP"));

        azureTableRangeQueryTaskSYS.setup(l,p);
    }

    @Override
    public void execute(Tuple input) {
        // path for both model files
//        String BlobModelPath = input.getStringByField("BlobModelPath");
//        String analyticsType = input.getStringByField("analyticsType");

        String msgId = input.getStringByField("MSGID");
        ROWKEYSTART= (input.getStringByField("ROWKEYSTART"));
        ROWKEYEND= (input.getStringByField("ROWKEYEND"));


        HashMap<String, String> map = new HashMap();
        map.put("ROWKEYSTART", ROWKEYSTART);
        map.put("ROWKEYEND", ROWKEYEND);

        Stopwatch stopwatch=null;
        if(l.isInfoEnabled()) {
            stopwatch = Stopwatch.createStarted(); //
        }
        /*
        //azureTableRangeQueryTaskSYS.doTask(map);
        //Iterable<AzureTableRangeQueryTaskSYS.SYS_City> result= (Iterable<AzureTableRangeQueryTaskSYS.SYS_City>) azureTableRangeQueryTaskSYS.getLastResult();
        if(l.isInfoEnabled()) {
            stopwatch.stop(); // optional
            l.info("Time elapsed for azureTableRangeQueryTask() is {}", stopwatch.elapsed(MILLISECONDS)); //
        }
	    String temp=null;
        StringBuffer bf=new StringBuffer();
        // Loop through the results, displaying information about the entity
        for (AzureTableRangeQueryTaskSYS.SYS_City entity : result) {
            bf  .append(entity.getTemperature()).append(",")
                .append(entity.getHumidity()).append(",")
                .append(entity.getLight()).append(",")
                .append(entity.getDust()).append(",")
                .append(entity.getAirquality_raw()).append("\n");
		        temp = entity.getDust();
        }
        */
        String inputFileName = "/home/cc/storm/riot-bench/modules/tasks/src/main/resources/SYS_sample_data_senml.csv";
        ReadFile readFile = new ReadFile(inputFileName, ROWKEYSTART, ROWKEYEND);
        StringBuffer bf = readFile.readDataSYS();
        if(l.isInfoEnabled())
            l.info("AZURE bf");
            //l.info("AZURE bf "+bf.toString());

        //FIXME: read and emit model for DTC
       if(bf.length() > 0) {
	        collector.emit(new Values(bf.toString(), msgId,ROWKEYEND));
	    }
    }
    
    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("TRAINDATA","MSGID","ROWKEYEND"));
    }

}
