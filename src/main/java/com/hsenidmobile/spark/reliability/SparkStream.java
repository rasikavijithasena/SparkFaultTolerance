package com.hsenidmobile.spark.reliability; /**
 * Created by cloudera on 10/23/17.
 */

import com.hsenidmobile.spark.reliability.util.ApplicationConf;
import com.hsenidmobile.spark.reliability.util.InputFieldValues;
import com.hsenidmobile.spark.reliability.util.Queries;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;

import org.apache.spark.sql.*;

import org.apache.spark.SparkConf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.*;


import static java.lang.Thread.sleep;

public class SparkStream implements Serializable {

    public static void main(String[] args) throws InterruptedException {

        SparkStream app = new SparkStream();
        InputFieldValues inputConf = new InputFieldValues();

        JavaStreamingContext streamingContext = app.getStreamingContext();
        SparkSession spark = app.getSession();

        app.createTable(spark, new Queries());

        JavaDStream<Row> inputStream = app.mapToTransaction(streamingContext);

        List<String> schemaString = inputConf.getSelectedFields();
        StructType schema = app.getSchema(schemaString);

        inputStream.foreachRDD(rdd -> {

            long startTime = System.currentTimeMillis();

            Dataset<Row> inputDataFrame = spark.createDataFrame(rdd, schema);
            //inputDataFrame.show();

            Dataset<Row> selectedDataSet = app.filterInput(inputDataFrame);
            selectedDataSet.show();
            //app.updateTable(selectedDataSet, spark);

            long endTime   = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            System.out.println(totalTime);

        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    public JavaStreamingContext getStreamingContext() {

        ApplicationConf conf = new ApplicationConf();
        String appName = conf.getAppName();
        String master = conf.getMaster();
        int duration = conf.getDuration();

        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(duration));  //batch interval parameter

        return streamingContext;
    }

    public SparkSession getSession() {
        ApplicationConf conf = new ApplicationConf();
        String appName = conf.getAppName();
        String hiveConf = conf.getHiveConf();
        String thriftConf = conf.getThriftConf();
        int shufflePartitions = conf.getShuffle();
        int parellelismPartitions = conf.getParellelismPartitions() ;

        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.sql.warehouse.dir", hiveConf)
                .config("hive.metastore.uris", thriftConf)
                .enableHiveSupport()
                .getOrCreate();


        spark.conf().set("spark.streaming.concurrentJobs","4");            // at a time 4 jobs are activated
        spark.conf().set("spark.streaming.kafka.maxRatePerPartition", "25");  //limit the maximum rate of messages/sec in a partition
        spark.conf().set("spark.driver.memory", "8g");
        spark.conf().set("spark.executor.memory", "15g");
        spark.conf().set("spark.streaming.unpersist","true");  //unpersist the dstream as soon as processing ends for the associated batch
        spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        spark.conf().set("spark.streaming.backpressure.enabled","true");   //slow down rate of sending messages
        spark.conf().set("spark.sql.shuffle.partitions", shufflePartitions);
        spark.conf().set("spark.default.parallelism", parellelismPartitions);

        return spark;

    }


    public StructType getSchema(List<String> schemaString) {

        //schemaString = "time_stamp,app_id,channel_type,travel_direction";

//      Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);

        }
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }


    public JavaDStream<Row> mapToTransaction(JavaStreamingContext streamingContext) {



        InputFieldValues inputConf = new InputFieldValues();
        String delimeter = inputConf.getDelimeter();
        String directory = inputConf.getDirectory() ;

        JavaDStream<String> logdata = streamingContext.textFileStream(directory);

        int numberOfFields = inputConf.getFieldCount();
        int numberOfSelectedFields = inputConf.getNumberOfSelectedFields();


        JavaDStream<Row> transStream = logdata.map(

                new Function<String, Row>() {
                    @Override
                    public Row call(String line) throws Exception {

                        String[] fieldValues = line.split(delimeter);
                        String[] trans = new String[numberOfSelectedFields];

                        int lineLength = fieldValues.length;
                        String fieldArray[] = new String[numberOfFields];

                        if (lineLength < numberOfFields) {
                            for (int i = 0; i < numberOfFields; i++) {
                                fieldArray[i] = " ";
                            }
                            for (int j = 0; j < lineLength; j++) {
                                fieldArray[j] = fieldValues[j];
                            }
                        } else {
                            fieldArray = fieldValues;
                        }


                        for (int counter = 0; counter < numberOfSelectedFields; counter++) {
                            int fieldNumber = inputConf.getFieldNumber(counter);
                            List<Integer> substring = inputConf.getSubString(counter);

                            if (substring.size() == 0) {
                                trans[counter] = fieldArray[fieldNumber - 1];
                            } else {
                                trans[counter] = fieldArray[fieldNumber - 1].substring(substring.get(0), substring.get(1));
                            }
                        }

                        return RowFactory.create(trans);

                    }
                });

        return transStream;

    }


    public Dataset<Row> filterInput(Dataset<Row> inputDataSet) {

        Queries query = new Queries();

        Dataset<Row> filteredDataset = inputDataSet.filter(query.getFilterStatement());
        Dataset<Row> groupedDataset = filteredDataset.groupBy(query.getGroupingField(1), query.getGroupingField(2)).count();

        return groupedDataset;

    }


    public void updateTable(Dataset<Row> outputDataFrame, SparkSession spark) {
        Queries queryConf = new Queries();

        if (outputDataFrame.count() != 0) {

            insertIntoTableFromDataset(spark, outputDataFrame, queryConf);

            Dataset<Row> groupedDataset = spark.sql(queryConf.groupingQuery());

            createView(spark, queryConf);
            insertIntoView(spark, groupedDataset, queryConf);

            truncateTable(spark, queryConf);
            createTable(spark, queryConf);

            insertIntoTableFromView(spark, queryConf);
            dropView(spark, queryConf);
        }

        printTable(spark, queryConf).show();


    }

    public Dataset<Row> printTable(SparkSession spark, Queries queryConf) {
        Dataset<Row> hiveDailySummaryDataset = spark.sql(queryConf.getPrintTableQuery());
        return hiveDailySummaryDataset;
    }


    public void truncateTable(SparkSession spark, Queries queryConf) {

        spark.sql(queryConf.getTruncateTableQuery());
    }

    public void createTable(SparkSession spark, Queries queryConf) {
        spark.sql(queryConf.getCreateTableQuery());
    }

    public void createView(SparkSession spark, Queries queryConf) {

        spark.sql(queryConf.getCreateViewQuery());
    }

    public void insertIntoTableFromView(SparkSession spark, Queries queryConf) {

        spark.sql(queryConf.insertDataFromTempViewQuery(queryConf.viewTableName));

    }

    public void insertIntoTableFromDataset(SparkSession spark, Dataset<Row> newDataSet, Queries queryConf) {

        newDataSet.createOrReplaceTempView("updateData");
        spark.sql(queryConf.insertDataFromTempViewQuery("updateData"));

    }

    public void insertIntoView(SparkSession spark, Dataset<Row> newDataSet, Queries queryConf) {

        newDataSet.createOrReplaceTempView("updateData");
        spark.sql(queryConf.getInsertViewQuery("updateData"));
    }

    public void dropView(SparkSession spark, Queries queryConf) {

        spark.sql(queryConf.getDropViewQuery());
    }

}








