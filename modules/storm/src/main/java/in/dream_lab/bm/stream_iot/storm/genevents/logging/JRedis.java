package in.dream_lab.bm.stream_iot.storm.genevents.logging;

import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;
//import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import java.util.ArrayList;
import java.util.List;

public class JRedis{
    int counter=0;
    List<TupleType> batch = new ArrayList<TupleType>();
    int threshold; //Count of rows after which the map should be flushed to log file
    String appName;
    Jedis jedis;
    Pipeline p;
    String HOSTNAME="127.0.0.1";  
    
    public JRedis(){
        this.threshold = GlobalConstants.thresholdFlushToLog; //2000 etc
    	this.appName = "/home/cc/test_test.log";
        this.jedis = new Jedis(this.HOSTNAME, 6379);
        this.p = jedis.pipelined(); 
 
    }
 
    public JRedis(String fileName){
        this.threshold = GlobalConstants.thresholdFlushToLog; //2000 etc
    	String[] fileName2 = fileName.split("/");
	String[] file = fileName2[fileName2.length-1].split("-");
	this.appName = file[1];
        this.jedis = new Jedis(this.HOSTNAME, 6379);
        this.p = jedis.pipelined(); 
    }
 
    public void batchWriter(long ts,String identifierData) throws Exception
    {
        if (counter<this.threshold)
        {
            batch.add(new TupleType(ts, identifierData));
            counter += 1;
        }
        else
        {
            //filePath = Properties p_.getProperty("ANNOTATE.ANNOTATE_FILE_PATH");
            for(TupleType tp : batch){
                long miliseconds = tp.ts % 60000;
                long minutes = (tp.ts - miliseconds)/1000;
                if(tp.identifier.contains("MSGID")){
                	//p.set(this.appName + "_"+tp.ts + "_" + tp.identifier, "-1");
                    // put all tuples to each application.
                    //p.hset(this.appName + "_spout", tp.identifier, String.valueOf(tp.ts));
                    // Group all tuples by each minute for each application.
                    this.p.hset(this.appName + "_spout_"+String.valueOf(minutes),
                            tp.identifier, String.valueOf(miliseconds));
                }else {
                    //p.set(this.appName + "_"+tp.ts + "_" + tp.identifier, String.valueOf(tp.ts));
                    //if the array is very big in redis, the latency accuracy will be decreasing dramatically.
                    // Therefore, we need to group tuples by each minute.
                    //p.hset(this.appName + "_sink", tp.identifier, String.valueOf(tp.ts));
                    this.p.hset(this.appName + "_sink_"+String.valueOf(minutes),
                            tp.identifier, String.valueOf(miliseconds));

                }

            }
	    this.p.sync();
            batch.clear();
            counter = 1 ;
            batch.add(new TupleType(ts, identifierData));
        }
    }

}
