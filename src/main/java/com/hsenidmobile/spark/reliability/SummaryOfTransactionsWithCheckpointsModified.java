package com.hsenidmobile.spark.reliability;

import com.hsenidmobile.spark.reliability.util.ApplicationConf;
import com.hsenidmobile.spark.reliability.util.InputFieldValues;
import com.hsenidmobile.spark.reliability.util.KafkaConfig;
import com.hsenidmobile.spark.reliability.util.Queries;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SummaryOfTransactionsWithCheckpointsModified implements Serializable{

    private static SparkSession sparkSession = null;

    //SummaryOfTransactionsWithCheckpointsModified streamingApplication = new SummaryOfTransactionsWithCheckpointsModified();



    public  static JavaStreamingContext createContextFunc() {

        SummaryOfTransactionsWithCheckpointsModified streamingApplication = new SummaryOfTransactionsWithCheckpointsModified();

        ApplicationConf conf = new ApplicationConf();
        String checkpointDir = conf.getCheckpointDirectory();

        JavaStreamingContext streamingContext =  streamingApplication.getStreamingContext(checkpointDir);

        JavaDStream<String> kafkaInputStream = streamingApplication.getKafkaInputStream(streamingContext);


        streamingApplication.printlogData(kafkaInputStream);

        return streamingContext;
    }




    public static void main(String[] args) throws InterruptedException {

        ApplicationConf conf = new ApplicationConf();
        String checkpointDir = conf.getCheckpointDirectory();

        Function0<JavaStreamingContext> createContextFunc = () -> createContextFunc();
        JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, createContextFunc);

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    public JavaStreamingContext getStreamingContext(String checkpointDir) {

        ApplicationConf conf = new ApplicationConf();
        String appName = conf.getAppName();
        String master = conf.getMaster();
        int duration = conf.getDuration();

        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master);
        sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(duration));
        streamingContext.checkpoint(checkpointDir);

        return streamingContext;
    }

    public static SparkSession getSparkSession() {

        ApplicationConf conf = new ApplicationConf();
        String appName = conf.getAppName();
        String hiveConf = conf.getHiveConf();
        String thriftConf =  conf.getThriftConf();
        int shufflePartitions = conf.getShuffle();

        if(sparkSession == null) {
            sparkSession = SparkSession
                    .builder()
                    .appName(appName)
                    .config("spark.sql.warehouse.dir", hiveConf)
                    .config("hive.metastore.uris", thriftConf)
                    .enableHiveSupport()
                    .getOrCreate();
        }

        sparkSession.conf().set("spark.sql.shuffle.partitions", shufflePartitions);
        return sparkSession;

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

    public JavaDStream<String> getKafkaInputStream(JavaStreamingContext streamingContext) {

        KafkaConfig kafkaConfig = new KafkaConfig();
        Set<String> topicsSet = kafkaConfig.getTopicSet();
        Map<String, Object> kafkaParams = kafkaConfig.getKafkaParams();

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        JavaDStream<String> logdata = messages.map(ConsumerRecord::value);

//        logdata.checkpoint(new Duration(6000));
        return logdata;
    }




    public void printlogData(JavaDStream<String> kafkaInputStream) {

        SummaryOfTransactionsWithCheckpointsModified streamingApplication = new SummaryOfTransactionsWithCheckpointsModified();


        InputFieldValues inputConf = new InputFieldValues();

        JavaDStream<Row> inputStream = streamingApplication.mapToTransaction(kafkaInputStream);

        List<String> schemaString = inputConf.getSelectedFields();
        StructType schema = streamingApplication.getSchema(schemaString);

        inputStream.foreachRDD(rdd -> printDataUsingRDD(rdd, schema));

    }

    public void printDataUsingRDD(JavaRDD<Row> logDataRDD, StructType schema) {

        SummaryOfTransactionsWithCheckpointsModified streamingApplication = new SummaryOfTransactionsWithCheckpointsModified();

        Dataset<Row> inputDataFrame = getSparkSession().createDataFrame(logDataRDD, schema);
        inputDataFrame.show();

        Dataset<Row> selectedDataSet = streamingApplication.filterInput(inputDataFrame);
        streamingApplication.updateTable(selectedDataSet,getSparkSession());
    }


    public JavaDStream<Row> mapToTransaction(JavaDStream<String> logdata) {


        InputFieldValues inputConf = new InputFieldValues();
        String delimeter = inputConf.getDelimeter();

        int numberOfFields = inputConf.getFieldCount();
        int numberOfSelectedFields = inputConf.getNumberOfSelectedFields();

        JavaDStream<Row> transStream = logdata.map(line -> splitLineToFields(line, numberOfFields, delimeter, inputConf, numberOfSelectedFields));

        return transStream;

    }

    public Row splitLineToFields(String line, int numberOfFields, String delimeter, InputFieldValues inputConf, int numberOfSelectedFields) {

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


        for (int counter = 0; counter < numberOfSelectedFields ; counter ++) {
            int fieldNumber = inputConf.getFieldNumber(counter);
            List<Integer> substring = inputConf.getSubString(counter);

            if(substring.size() == 0){
                trans[counter] = fieldArray[fieldNumber-1];
            } else {
                trans[counter] = fieldArray[fieldNumber-1].substring(substring.get(0), substring.get(1));
            }
        }

        return RowFactory.create(trans);
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

            insertIntoTableFromDataset(spark, outputDataFrame , queryConf);

            Dataset<Row> groupedDataset = spark.sql(queryConf.groupingQuery());

            createView(spark, queryConf);
            insertIntoView(spark, groupedDataset, queryConf);

            truncateTable(spark, queryConf);
            createTable(spark, queryConf);

            insertIntoTableFromView(spark,queryConf);
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

    public void insertIntoView(SparkSession spark, Dataset<Row> newDataSet , Queries queryConf) {

        newDataSet.createOrReplaceTempView("updateData");
        spark.sql(queryConf.getInsertViewQuery("updateData"));
    }

    public void dropView(SparkSession spark, Queries queryConf) {

        spark.sql(queryConf.getDropViewQuery());
    }

    public static class Line{
        private String line;


        public String getLine() {
            return line;
        }

        public void setLine(String line) {
            this.line = line;
        }
    }

    public void createTransactionTable(SparkSession spark) {

        spark.sql("create table if not exists fullLine(line String)");

    }

    public void insertStream(SparkSession spark, Dataset<Row> linesDf ) {
        linesDf.createOrReplaceTempView("lines");
        spark.sql("INSERT INTO fullLine SELECT * FROM lines");
    }


}
