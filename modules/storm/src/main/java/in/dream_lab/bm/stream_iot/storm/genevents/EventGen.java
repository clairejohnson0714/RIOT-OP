package in.dream_lab.bm.stream_iot.storm.genevents;

//import main.in.dream_lab.genevents.factory.CsvSplitter;
//import main.in.dream_lab.genevents.factory.TableClass;
//import main.in.dream_lab.genevents.utils.GlobalConstants;

import in.dream_lab.bm.stream_iot.storm.genevents.factory.CsvSplitter;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.TableClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.JsonSplitter;
import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventGen {
    ISyntheticEventGen iseg;
    ExecutorService executorService;
    double scalingFactor;

    public EventGen(ISyntheticEventGen iseg) {
        this(iseg, GlobalConstants.accFactor);
    }

    public EventGen(ISyntheticEventGen iseg, double scalingFactor) {
        this.iseg = iseg;
        this.scalingFactor = scalingFactor;
    }

    public static List<String> getHeadersFromCSV(String csvFileName) {
        return CsvSplitter.extractHeadersFromCSV(csvFileName);
    }

    public void launch(String csvFileName, String outCSVFileName)
    {
    	launch(csvFileName, outCSVFileName, -1);
    }

    //Launches all the threads
    public void launch(String csvFileName, String outCSVFileName, long experimentDurationMillis) {
        //1. Load CSV to in-memory data structure
        //2. Assign a thread with (new SubEventGen(myISEG, eventList))
        //3. Attach this thread to ThreadPool
        try {
            int numThreads = GlobalConstants.numThreads;
            //double scalingFactor = GlobalConstants.accFactor;
            String datasetType = "";
            if (outCSVFileName.indexOf("PLUG") != -1) {
                datasetType = "PLUG";// GlobalConstants.dataSetType = "PLUG";
            }
	    else if (outCSVFileName.indexOf("TAXI") != -1) {
                datasetType = "TAXI";// GlobalConstants.dataSetType = "TAXI";
            } else if (outCSVFileName.indexOf("SYS") != -1) {
                datasetType = "SYS";// GlobalConstants.dataSetType = "SYS";
            }             
            List<TableClass> nestedList = CsvSplitter.roundRobinSplitCsvToMemory(csvFileName, numThreads, scalingFactor, datasetType);
            
            
            this.executorService = Executors.newFixedThreadPool(numThreads);

            Semaphore sem1 = new Semaphore(0);

            Semaphore sem2 = new Semaphore(0);

            SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
            for (int i = 0; i < numThreads; i++) {
                //this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), sem1, sem2);
                this.executorService.execute(subEventGenArr[i]);
            }

            sem1.acquire(numThreads);
            //set the start time to all the thread objects
            long experiStartTs = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                //this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i].experiStartTime = experiStartTs;
                if (experimentDurationMillis > 0) subEventGenArr[i].experiDuration = experimentDurationMillis;
                this.executorService.execute(subEventGenArr[i]);
            }
            sem2.release(numThreads);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void launch(String csvFileName, String outCSVFileName, long experimentDurationMillis, boolean isJson) {
        //1. Load CSV to in-memory data structure
        //2. Assign a thread with (new SubEventGen(myISEG, eventList))
        //3. Attach this thread to ThreadPool
        try {
            int numThreads = GlobalConstants.numThreads;
            //double scalingFactor = GlobalConstants.accFactor;
            String datasetType = "";
            if (outCSVFileName.indexOf("PLUG") != -1) {
                datasetType = "PLUG";// GlobalConstants.dataSetType = "PLUG";
            }
            else if (outCSVFileName.indexOf("SENML") != -1) {
                datasetType = "SENML";// GlobalConstants.dataSetType = "SENML";
            }
	    else if (outCSVFileName.indexOf("TAXI") != -1) {
                datasetType = "TAXI";// GlobalConstants.dataSetType = "TAXI";
            } else if (outCSVFileName.indexOf("SYS") != -1) {
                datasetType = "SYS";// GlobalConstants.dataSetType = "SYS";
            }   
            List<TableClass> nestedList = JsonSplitter.roundRobinSplitJsonToMemory(csvFileName, numThreads, scalingFactor, datasetType);
          
            this.executorService = Executors.newFixedThreadPool(numThreads);

            Semaphore sem1 = new Semaphore(0);

            Semaphore sem2 = new Semaphore(0);

            SubEventGen[] subEventGenArr = new SubEventGen[numThreads];
            for (int i = 0; i < numThreads; i++) {
                //this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i] = new SubEventGen(this.iseg, nestedList.get(i), sem1, sem2);
                this.executorService.execute(subEventGenArr[i]);
            }

            sem1.acquire(numThreads);
            //set the start time to all the thread objects
            long experiStartTs = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                //this.executorService.execute(new SubEventGen(this.iseg, nestedList.get(i)));
                subEventGenArr[i].experiStartTime = experiStartTs;
                if (experimentDurationMillis > 0) subEventGenArr[i].experiDuration = experimentDurationMillis;
                this.executorService.execute(subEventGenArr[i]);
            }
            sem2.release(numThreads);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}

class SubEventGen implements Runnable {
    ISyntheticEventGen iseg;
    TableClass eventList;
    Long experiStartTime;  //in millis since epoch
    Semaphore sem1, sem2;
    Long experiDuration = -1L;
	
    public SubEventGen(ISyntheticEventGen iseg, TableClass eventList, Semaphore sem1, Semaphore sem2) {
        this.iseg = iseg;
        this.eventList = eventList;
        this.sem1 = sem1;
        this.sem2 = sem2;
    }

    @Override
    public void run() {

        sem1.release();
        try {
            sem2.acquire();
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        List<List<String>> rows = this.eventList.getRows();
        int rowLen = rows.size();
        List<Long> timestamps = this.eventList.getTs();
        Long experiRestartTime = experiStartTime;
	    experiDuration = 1000000000L;
	    boolean runOnce = (experiDuration < 0);
        long currentRuntime = 0;


	    long windowStartTime = System.currentTimeMillis();
	    //long[] dynamicWorkloadWindow = {300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300,  150, 150, 150, 150, 150, 150, 150, 150, 150, 150,150, 150, 150, 150, 150, 150, 150, 150, 150, 150,150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 100, 100,100, 100, 100, 100, 100, 100, 100, 100, 100, 100,100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150, 150 };
        //long[] dynamicWorkloadWindow = {
        // 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512,
        // 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643,
        // 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643, 643,
//				324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324,
//				324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 324, 186, 186, 186, 186, 186, 186, 186, 186,
//				186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186, 186,
//				186, 186, 186, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145,
//				145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 145, 137, 137, 137, 137, 137,
//				137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137, 137,
//				137, 137, 137, 137, 137, 137, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140,
//				140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 140, 136, 136,
//				136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136, 136,
//				136, 136, 136, 136, 136, 136, 136, 136, 136, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129,
//				129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129, 129,
//				129, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130,
//				130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 130, 125, 125, 125, 125, 125, 125, 125,
//				125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125, 125,
//				125, 125, 125, 125, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131,
//				131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 131, 155, 155, 155, 155,
//				155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155, 155,
//				155, 155, 155, 155, 155, 155, 155, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127,
//				127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 103,
//				103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 103,
//				103, 103, 103, 103, 103, 103, 103, 103, 103, 103, 100, 100, 100, 100, 100, 100, 100, 100, 100,
//				100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
//				100, 100, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105,
//				105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 105, 109, 109, 109, 109, 109, 109,
//				109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109, 109,
//				109, 109, 109, 109, 109, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113,
//				113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 113, 128, 128, 128,
//				128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
//				128, 128, 128, 128, 128, 128, 128, 128, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156,
//				156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156, 156,
//				211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211,
//				211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 211, 300, 300, 300, 300, 300, 300, 300, 300,
//				300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300,
//				300, 300, 300, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370,
//				370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370, 370};
	    long[] dynamicWorkloadWindow = {100, 100, 100, 100, 100, 100};
        //produce random workload 
        //long[] randomDynamicBase = {300, 250, 150, 125, 100, 300};	
        /*
        Random rd = new Random(); // creating Random object
        long[] dynamicWorkloadWindow = new long[15*40];
        int temp = 0;
        for (int i = 0; i < dynamicWorkloadWindow.length; i++) {
            if(i%40 == 0) temp = (rd.nextInt()%5+5)%5; // storing random integers in an array
            dynamicWorkloadWindow[i] = randomDynamicBase[temp];
        }

        */
	    //{60, 60, 30, 20, 15, 12, 10,10, 12, 15, 20, 30, 60};

        //poission paramater is 5.
	    long[] poissionWindow = {10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,10,10, 10};
 	    //long[] poissionWindow = {250, 125, 77, 63, 59, 77, 100, 166, 250, 500, 600, 600, 600, 600, 600};
         //long[] poissionWindow = {
         //        23, 12, 7, 5, 5, 6, 9, 15, 29, 54, 116, 277, 526, 2500, 10000
         //};

        long bigWindowStartTime = windowStartTime;
        int bigLoc =0;
	
	    int poissionLoc = 0;
	    long poissionPara = poissionWindow[poissionLoc]*dynamicWorkloadWindow[bigLoc]/100;

        do {
            for (int i = 0; i < rowLen && (runOnce || (currentRuntime < experiDuration)); i++) {
		
                Long deltaTs = timestamps.get(i);
                List<String> event = rows.get(i);
                Long currentTs = System.currentTimeMillis();

		        if (currentTs - bigWindowStartTime >= 60000){
			        bigWindowStartTime = currentTs;
			        bigLoc += 1;
			        if (bigLoc == dynamicWorkloadWindow.length) bigLoc = 0;
			        break;
		        }
		        if (currentTs - windowStartTime >= 4000){
			        windowStartTime = currentTs;
			        poissionLoc += 1;
			        if (poissionLoc == poissionWindow.length ) poissionLoc = 0;
			        poissionPara = poissionWindow[poissionLoc]*dynamicWorkloadWindow[bigLoc]/100;
                    break;
		        }

		        //System.out.printf("GEVENT time "+ String.valueOf(deltaTs));
 
                deltaTs = (long)(deltaTs *poissionPara/1000);
		
		        long delay = deltaTs - (currentTs - experiRestartTime); // how long until this event should be sent?
		
		        if (delay > 10) { // sleep only if it is non-trivial time. We will catch up on sleep later.
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                this.iseg.receive(event);

                currentRuntime = (currentTs - experiStartTime) + delay; // appox time since the experiment started
            }

            experiRestartTime = System.currentTimeMillis();
        } while (!runOnce && (currentRuntime < experiDuration));

    }
}
