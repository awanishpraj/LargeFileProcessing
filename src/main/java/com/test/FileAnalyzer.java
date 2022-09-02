package com.test;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.test.DataBaseUtil.createTable;
import static com.test.DataBaseUtil.insertEventQuery;

/**
 * Objective : (Find events taking longer time to finish)
 * This program will extract "timestamp" value for the event's "id" and its START & FINISHED "status" from the JSON
 * and provide info about events taking longer time than usual to finish.
 */


/**
 *
 * The program should:
 *  Take the path to logfile.txt as an input argument
 *  Parse the contents of logfile.txt
 *  Flag any long events that take longer than 4ms
 *  Write the found event details to file-based HSQLDB (http://hsqldb.org/) in the working folder
 *  The application should create a new table if necessary and store the following values:
 *  Event id
 *  Event duration
 *  Type and Host if applicable
 *  Alert (true if the event took longer than 4ms, otherwise false)
 *
 *
 ***** Additional points will be granted for:*****
 *  Proper use of info and debug logging
 *  Proper use of Object Oriented programming
 *  Unit test coverage
 *  Multi-threaded solution
 *  Program that can handle very large files (gigabytes)
 *
 *
 */


public class FileAnalyzer {

    private static Logger logger = LoggerFactory.getLogger(FileAnalyzer.class);
    public final static String INPUT_FILE_PATH = "src/main/resources/logfile.txt";
    public final static String ALERT_FILE_PATH = "src/main/resources/alertData.txt";

    enum FieldsToExtract{id,timestamp,state,type,host}

    public static void main(String [] str) throws IOException {


        File file = new File(INPUT_FILE_PATH);
        File alertFile = new File(ALERT_FILE_PATH);
        JSONParser parser = new JSONParser();
        final JSONObject[] jsonResponse = new JSONObject[1];
        ConcurrentHashMap<String, Map<String, String>> dataMap = new ConcurrentHashMap<>();

        /*
         * Ready with the table created to insert all events duration detains in the event_tbl
         */
        int t = createTable();
        if (t != 0) {
            logger.info("Event table : {} got created...{}", "event_tbl", t);
        } else {
            logger.error("Event table : {} did not created. {}", "event_tbl", t);
        }
        /*
         * In Java8 : java.nio.file.Files got introduce, it has got maximum performance while processing large files
         * http://hsqldb.org/
         */
        try (Stream linesStream = Files.lines(file.toPath())) {
            linesStream.forEach(line -> {

                //System.out.println(line);

                try {
                    jsonResponse[0] = (JSONObject)parser.parse(line.toString());
                    //logger.info(jsonResponse[0].toString());
                   // System.out.println(jsonResponse[0].get("id")+" :: "+ jsonResponse[0].get("state") +" :: "+ jsonResponse[0].get("timestamp")+" :: "+ (jsonResponse[0].containsKey("host") ? jsonResponse[0].get("host"): "") +" :: "+ (jsonResponse[0].containsKey("type") ? jsonResponse[0].get("type") : ""));
                    /**Add event id in the outerMap if it is NOT present in it. If present, that means START or its FINISHED event id is already read
                     * and stored in the outerMap. Now we need to find the difference of timestamp of both and this is done in the else condition
                    */
                    if (dataMap.get(jsonResponse[0].get("id").toString()) == null) {
                        final Map<String, String>[] innerData = new HashMap[1];
                        innerData[0] = new HashMap<>();
                        Arrays.stream(FieldsToExtract.values()).iterator().forEachRemaining(k -> {
                            if(jsonResponse[0].get(k.toString()) != null) {
                                innerData[0].put(k.toString(), jsonResponse[0].get(k.toString()).toString());
                            }
                        });
                        dataMap.put(jsonResponse[0].get("id").toString(), innerData[0]);
                        Arrays.stream(FieldsToExtract.values()).forEach(k -> logger.info(innerData[0].get(k.toString())));
                    } else {
                        /**
                         * Calculate the duration to finish the event
                         * Insert the event data in the table
                         */
                        //System.out.println(Timestamp.valueOf(dataMap.get(jsonResponse[0].get("id").toString()).get(FieldsToExtract.timestamp.toString())));

                        if(jsonResponse[0].get("state").toString().equalsIgnoreCase("STARTED")){
                            Long duration = getDuration(dataMap.get(jsonResponse[0].get("id").toString()).get(FieldsToExtract.timestamp.toString()), jsonResponse[0].get("timestamp").toString(),dataMap, jsonResponse);
                            insertEventQuery(new String[]{jsonResponse[0].get("id").toString(), String.valueOf(duration), (jsonResponse[0].get("type") != null) ? jsonResponse[0].get("type").toString() : "", (jsonResponse[0].get("host") != null) ? jsonResponse[0].get("host").toString() : ""});
                            //String string = String.format("Event id : %s  ||  Event Duration : %d  ||  Type : %s  ||  Host : %s", jsonResponse[0].get("id").toString(), Long.parseLong(dataMap.get(jsonResponse[0].get("id").toString()).get(FieldsToExtract.timestamp.toString())) - (Long.parseLong(jsonResponse[0].get("timestamp").toString())), (jsonResponse[0].get("type")!=null)?jsonResponse[0].get("type").toString():"", (jsonResponse[0].get("host")!=null)?jsonResponse[0].get("host").toString():"");
                            if (duration >= 4) {
                                alert(alertFile, dataMap, jsonResponse, duration);
                            }
                            /*
                             * After print/write, Now remove the data from map to save memory
                             */
                            removeMapData(dataMap, jsonResponse);
                        } else {
                            Long duration = getDuration(jsonResponse[0].get("timestamp").toString(), dataMap.get(jsonResponse[0].get("id").toString()).get(FieldsToExtract.timestamp.toString()),dataMap, jsonResponse);
                            //String[] strA = new String[]{jsonResponse[0].get("id").toString(), String.valueOf(duration), (jsonResponse[0].get("type") != null) ? jsonResponse[0].get("type").toString() : "", (jsonResponse[0].get("host") != null) ? jsonResponse[0].get("host").toString() : ""};
                            insertEventQuery(new String[]{jsonResponse[0].get("id").toString(), String.valueOf(duration), (jsonResponse[0].get("type") != null) ? jsonResponse[0].get("type").toString() : "", (jsonResponse[0].get("host") != null) ? jsonResponse[0].get("host").toString() : ""});
                            //insertEventQuery(strA);
                            if (duration >= 4) {
                                alert(alertFile, dataMap, jsonResponse, duration);
                            }

                            removeMapData(dataMap, jsonResponse);
                        }

                    };

                } catch (ParseException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        }
    }

    static Long getDuration(String finished, String start, ConcurrentHashMap<String, Map<String, String>> dataMap, JSONObject[] jsonResponse) {
        Long duration = Long.parseLong(finished) - Long.parseLong(start);
        logger.info("Event id : {}, Event duration DIFFERENCE_OF({}, {}) => {}", jsonResponse[0].get("id").toString(), jsonResponse[0].get("timestamp"), dataMap.get(jsonResponse[0].get("id").toString()).get(FieldsToExtract.timestamp.toString()), duration);
        return duration;
    }

    static void alert(File alertFile, ConcurrentHashMap<String, Map<String, String>> dataMap, JSONObject[] jsonResponse, Long duration) throws IOException {
        Files.write(alertFile.toPath(), String.format("\n Event id : %s  ||  Event Duration : %d  ||  Type : %s  ||  Host : %s", jsonResponse[0].get("id").toString(), duration, (jsonResponse[0].get("type") != null) ? jsonResponse[0].get("type").toString() : "", (jsonResponse[0].get("host") != null) ? jsonResponse[0].get("host").toString() : "").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
    }
    static void removeMapData(ConcurrentHashMap<String, Map<String, String>> dataMap, JSONObject[] jsonResponse) {
        logger.info("Removing processed event data from map for the event id: {}", dataMap.get(jsonResponse[0].get("id").toString()));
        dataMap.get(jsonResponse[0].get("id").toString()).remove(FieldsToExtract.timestamp.toString());
        dataMap.remove(jsonResponse[0].get("id").toString());
    }

}

