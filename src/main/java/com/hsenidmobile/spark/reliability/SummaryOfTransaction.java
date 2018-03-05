package com.hsenidmobile.spark.reliability;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
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

import com.hsenidmobile.spark.reliability.util.ApplicationConf;
import com.hsenidmobile.spark.reliability.util.InputFieldValues;
import com.hsenidmobile.spark.reliability.util.KafkaConfig;
import com.hsenidmobile.spark.reliability.util.Queries;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SummaryOfTransaction implements Serializable{

    private static final String CHECKPOINT_DIRECTORY = "/home/cloudera/Documents/check";

    public  static JavaStreamingContext createContextFunc() {

        String checkpointDir = "/home/cloudera/Documents/checkpoints";
        SummaryOfTransaction app = new SummaryOfTransaction();
        InputFieldValues inputConf = new InputFieldValues();

        JavaStreamingContext streamingContext =  app.getStreamingContext(checkpointDir);

        JavaDStream<String> kafkaInputStream = app.getKafkaInputStream(streamingContext);

        app.printlogData(kafkaInputStream);



        JavaDStream<Row> inputStream = app.mapToTransaction(kafkaInputStream);
        List<String> schemaString = inputConf.getSelectedFields();
        StructType schema = app.getSchema(schemaString);

        inputStream.foreachRDD(rdd -> {

            SparkSession spark = app.getSession();
            app.createTable(spark, new Queries());

            if(!rdd.isEmpty()) {
                Dataset<Row> inputDataFrame = spark.createDataFrame(rdd, schema);
                inputDataFrame.show();

                Dataset<Row> selectedDataSet = app.filterInput(inputDataFrame);
                app.updateTable(selectedDataSet,spark);
            }

        });

        return streamingContext;
    }




    public static void main(String[] args) throws InterruptedException {

        String checkpointDir = "/home/cloudera/Documents/checkpoints";
        SummaryOfTransaction app = new SummaryOfTransaction();
        InputFieldValues inputConf = new InputFieldValues();

//        Function0<JavaStreamingContext> createContextFunc = () -> app.getStreamingContext(checkpointDir);
//        JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, createContextFunc);
//
        Function0<JavaStreamingContext> createContextFunc = () -> createContextFunc();
        JavaStreamingContext streamingContext = JavaStreamingContext.getOrCreate(checkpointDir, createContextFunc);



        /*JavaStreamingContext streamingContext = app.getStreamingContext(checkpointDir);
        SparkSession spark = app.getSession();

        app.createTable(spark, new Queries());

        JavaDStream<String> kafkaInputStream = app.getKafkaInputStream(streamingContext);

        JavaDStream<Row> inputStream = app.mapToTransaction(kafkaInputStream);
        List<String> schemaString = inputConf.getSelectedFields();
        StructType schema = app.getSchema(schemaString);

        inputStream.foreachRDD(rdd -> {
            Dataset<Row> inputDataFrame = spark.createDataFrame(rdd, schema);
            inputDataFrame.show();

            Dataset<Row> selectedDataSet = app.filterInput(inputDataFrame);
            app.updateTable(selectedDataSet,spark);

        });*/

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    public JavaStreamingContext getStreamingContext(String checkpointDir) {

        ApplicationConf conf = new ApplicationConf();
        String appName = conf.getAppName();
        String master = conf.getMaster();
        int duration = conf.getDuration();

        SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master).set("spark.streaming.receiver.writeAheadLog.enable", "true");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(duration));
        streamingContext.checkpoint(checkpointDir);

        return streamingContext;
    }

    public SparkSession getSession() {

        ApplicationConf conf = new ApplicationConf();
        String appName = conf.getAppName();
        String hiveConf = conf.getHiveConf();
        String thriftConf =  conf.getThriftConf();
        int shufflePartitions = conf.getShuffle();

        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .config("spark.sql.warehouse.dir", hiveConf)
                .config("hive.metastore.uris", thriftConf)
                .enableHiveSupport()
                .getOrCreate();

        spark.conf().set("spark.sql.shuffle.partitions", shufflePartitions);
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

    public JavaDStream<String> getKafkaInputStream(JavaStreamingContext streamingContext) {

        KafkaConfig kafkaConfig = new KafkaConfig();
        Set<String> topicsSet = kafkaConfig.getTopicSet();
        Map<String, Object> kafkaParams = kafkaConfig.getKafkaParams();

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> logdata = messages.map(ConsumerRecord::value);
        logdata.checkpoint(new Duration(2000));
        return logdata;
    }




    public void printlogData(JavaDStream<String> logdata) {

        InputFieldValues inputConf = new InputFieldValues();
        String delimeter = inputConf.getDelimeter();

        int numberOfFields = inputConf.getFieldCount();
        int numberOfSelectedFields = inputConf.getNumberOfSelectedFields();

        logdata.print();

        JavaDStream<String> transStream = logdata.map(

                new Function<String, String>() {
                    @Override
                    public String call(String line) throws Exception {

                        System.out.println("hi " +line);
                        return line;

                    }
                });


        transStream.foreachRDD(rdd -> {
            System.out.println("1dsdsk");
            System.out.println(rdd);
            rdd.checkpoint();

        });

        //return transStream;

    }

    public JavaDStream<Row> mapToTransaction(JavaDStream<String> logdata) {


        InputFieldValues inputConf = new InputFieldValues();
        String delimeter = inputConf.getDelimeter();

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




    public static StructType getSchema() {
        return new StructType()
                .add("id", "string")
                .add("time_stamp", "string")
                .add("sp_id", "string")
                .add("service_provider", "string")
                .add("app_id", "string")
                .add("app_name", "string")
                .add("state_app", "string")
                .add("source_entity_address", "string")
                .add("source_entity_masked", "string")
                .add("channel_type", "string")

                .add("source_protocol", "string")
                .add("dest_address", "string")
                .add("dest_masked", "string")
                .add("dest_channel_type", "string")
                .add("dest_protocol", "string")
                .add("travel_direction", "string")
                .add("ncs", "string")
                .add("billing", "string")
                .add("part_entity_type", "string")
                .add("charge_amount", "string")

                .add("currency", "string")
                .add("exchange_rates", "string")
                .add("charging_service_code", "string")
                .add("msisdn", "string")
                .add("masked_msisdn", "string")
                .add("billing_event", "string")
                .add("response_code", "string")
                .add("response_desc", "string")
                .add("transaction_state", "string")
                .add("transaction_keyword", "string")

                .add("col_31", "string")
                .add("col_32", "string")
                .add("col_33", "string")
                .add("col_34", "string")
                .add("col_35", "string")
                .add("col_36", "string")
                .add("col_37", "string")
                .add("col_38", "string")
                .add("col_39", "string")
                .add("col_40", "string")


                .add("col_41", "string")
                .add("col_42", "string")
                .add("col_43", "string")
                .add("col_44", "string")
                .add("col_45", "string")
                .add("col_46", "string")
                .add("col_47", "string")
                .add("col_48", "string")
                .add("col_49", "string")
                .add("col_50", "string")

                .add("col_51", "string")
                .add("col_52", "string")
                .add("col_53", "string")
                .add("col_54", "string")
                .add("col_55", "string")
                .add("col_56", "string")
                .add("col_57", "string")
                .add("col_58", "string")
                .add("col_59", "string")
                .add("col_60", "string");

    }


}
