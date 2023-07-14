package in.dream_lab.bm.stream_iot.storm.genevents.factory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ReadFile {

    private String inputFileName = null;
    private String rowKeyStart = null;
    private String rowKeyEnd = null;
    FileReader reader = null;
    BufferedReader bufferedReader = null;

    public ReadFile(){}

    public ReadFile(String inputFileName, String ROWKEYSTART, String ROWKEYEND){
        this.inputFileName = inputFileName;
        this.rowKeyStart = ROWKEYSTART;
        this.rowKeyEnd = ROWKEYEND;
    }
    public StringBuffer readDataTaxi(){
        String nextLine = null;
        StringBuffer bf = new StringBuffer();

        try {
            reader = new FileReader(this.inputFileName);
            bufferedReader = new BufferedReader(reader);
            nextLine = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Loop through the results, displaying information about the entity

        int i = 0;
        while (nextLine != null) {
            String[] values = new String[2];
            values[0] = (nextLine.split(","))[0];
            values[1] = nextLine.substring(nextLine.indexOf(",") + 1);
            //if (Long.parseLong(values[0]) > Long.parseLong(this.rowKeyStart)
            //        && Long.parseLong(values[0]) < Long.parseLong(this.rowKeyEnd))
            {
                JsonParser parser = new JsonParser();
                JsonObject object = (JsonObject) parser.parse(values[1]);
                String tripTimeInSecs = null;
                String tripDistance = null;
                String fareAmount = null;
                for (JsonElement element : object.get("e").getAsJsonArray())
                {
                    if (element.getAsJsonObject().get("n").getAsString().equals("trip_time_in_secs"))
                        tripTimeInSecs = element.getAsJsonObject().get("v").getAsString();
                    if (element.getAsJsonObject().get("n").getAsString().equals("trip_distance"))
                        tripDistance = element.getAsJsonObject().get("v").getAsString();
                    if (element.getAsJsonObject().get("n").getAsString().equals("fare_amount"))
                        fareAmount = element.getAsJsonObject().get("v").getAsString();
                }
                bf.append(tripTimeInSecs).append(",")
                        .append(tripDistance).append(",")
                        .append(fareAmount).append("\n");
            }
            if ( i > 500)
                break;
            i ++;
        }
        return bf;
    }

    public StringBuffer readDataSYS(){
        String nextLine = null;
        try {
            reader = new FileReader(this.inputFileName);
            bufferedReader = new BufferedReader(reader);
            nextLine = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuffer bf = new StringBuffer();
        // Loop through the results, displaying information about the entity
        int i = 0;
        while (nextLine != null) {
            String[] values = new String[2];
            values[0] = (nextLine.split(","))[0];
            values[1] = nextLine.substring(nextLine.indexOf(",") + 1);
            //if (Long.parseLong(values[0]) > Long.parseLong(this.rowKeyStart)
            //        && Long.parseLong(values[0]) < Long.parseLong(this.rowKeyEnd))
            {
                JsonParser parser = new JsonParser();
                JsonObject object = (JsonObject) parser.parse(values[1]);
                String temperature = null;
                String humidity = null;
                String light = null;
                String dust = null;
                String airQuality = null;

                for (JsonElement element : object.get("e").getAsJsonArray())
                {
                    if (element.getAsJsonObject().get("n").getAsString().equals("temperature"))
                        temperature = element.getAsJsonObject().get("v").getAsString();
                    if (element.getAsJsonObject().get("n").getAsString().equals("humidity"))
                        humidity = element.getAsJsonObject().get("v").getAsString();
                    if (element.getAsJsonObject().get("n").getAsString().equals("light"))
                        light = element.getAsJsonObject().get("v").getAsString();
                    if (element.getAsJsonObject().get("n").getAsString().equals("dust"))
                        dust = element.getAsJsonObject().get("v").getAsString();
                    if (element.getAsJsonObject().get("n").getAsString().equals("airquality_raw"))
                        airQuality = element.getAsJsonObject().get("v").getAsString();
                }
                bf.append(temperature).append(",")
                        .append(humidity).append(",")
                        .append(light).append(",")
                        .append(dust).append(",")
                        .append(airQuality).append("\n");
            }
            if ( i > 500)
                break;
            i ++;
        }
        return bf;
    }

}
