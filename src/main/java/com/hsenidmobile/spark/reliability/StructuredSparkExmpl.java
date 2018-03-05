package com.hsenidmobile.spark.reliability;

import com.hsenidmobile.spark.reliability.util.ApplicationConf;
import com.hsenidmobile.spark.reliability.util.InputFieldValues;
import com.hsenidmobile.spark.reliability.util.KafkaConfig;
import com.hsenidmobile.spark.reliability.util.StructuredConf;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.ScalaReflection;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StructuredSparkExmpl implements Serializable {

    public static void main(String[] args) throws StreamingQueryException {

        StructuredSparkExmpl app = new StructuredSparkExmpl();
        StructuredConf conf = new StructuredConf();

        String master = conf.getMaster();
        int shuffle = conf.getShuffle();
        String path = conf.getParquetPath();
        String checkpointPath = conf.getCheckpointPath();


        SparkSession spark = app.getSession(master, shuffle);
        Dataset<String> dataset = app.getInputDataset(spark);

        Dataset<Transaction> mappedDataset = app.mapToTransaction(dataset);

        Dataset<Row> selectedDataset = mappedDataset
                .select("id", "sp_id", "app_id", "time_stamp")
                .where("channel_type = 'sms' AND travel_direction = 'mo'");
                //.withWatermark("timestamp", "10 minutes")
                //.groupBy("app_id")
                //.count();



        spark.sparkContext().setLocalProperty("spark.scheduler.pool", "pool1");
        StreamingQuery streamingQuery = selectedDataset
                .writeStream()
                .format("parquet")
                .option("path", path)
                .option("checkpointLocation", checkpointPath)
                .start();

        spark.sparkContext().setLocalProperty("spark.scheduler.pool", "pool2");
        StreamingQuery streamingQuery2 = selectedDataset
                .writeStream()
                .format("console")
                .start();

        streamingQuery.awaitTermination();
        //streamingQuery2.awaitTermination();
    }

    public SparkSession getSession(String master, int shuffle) {

        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("StructuredSparkExmpl")
                .getOrCreate();
        //spark.conf().set("spark.sql.shuffle.partitions", shuffle);
        return spark;
    }



    public Dataset<String> getInputDataset(SparkSession spark) {

        StructuredConf conf = new StructuredConf();
        String bootstrap = conf.getBootstrap();
        String topic = conf.getTopic();

        Dataset<String> dataset = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", topic)
                .load()
                //.selectExpr("CAST(value AS STRING) " , "CAST(timestamp AS timestamp)")
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        return dataset;

    }

    public Dataset<String> getInputDatasetFromKafka(SparkSession spark) {

        StructuredConf conf = new StructuredConf();
        StructuredSparkExmpl app = new StructuredSparkExmpl();
        String bootstrap = conf.getBootstrap();
        String topic = conf.getTopic();
        StructType schema = app.getSchema();

        List <String> list = new ArrayList<>();
        list.add("www");

        Set<String> newSet = new HashSet<>();
        newSet.add("T");


        //schema.apply(newSet);



        Dataset<Row> dataset = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", topic)
                .load();

                //.selectExpr("CAST(value AS STRING) " , "CAST(timestamp AS timestamp)")
         Dataset<String> ds =  dataset
                 .selectExpr("CAST(value AS STRING)")
                 .as(Encoders.STRING());
                 //.map(x -> schema(x[0], x[1], x[2],  x[3], x[4], x[5]))


        //Dataset<Row> x = ds.map()



        return ds;

    }

   /* public Dataset<Row> mapToTrans(Dataset<String> inputDataset) {



        InputFieldValues inputConf = new InputFieldValues();
        String delimeter = inputConf.getDelimeter();
        String directory = inputConf.getDirectory() ;
        StructuredSparkExmpl app = new StructuredSparkExmpl();


        int numberOfFields = inputConf.getFieldCount();
        int numberOfSelectedFields = inputConf.getNumberOfSelectedFields();


        Dataset<Row> transStream = inputDataset.map(

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

                        StructType schema = app.getSchema();
                        schema.apply(trans);
                       // return schema.toSet();

                        //return RowFactory.create(trans);

                    }
                },Encoders.bean(Row.class));

        return transStream;

    }

*/  // with errors

    public Dataset<Transaction> mapToTransaction(Dataset<String> inputDataset) {

        InputFieldValues inputConf = new InputFieldValues();
        String delimeter = inputConf.getDelimeter();

        Dataset<Transaction> tranStream = inputDataset
                .map(new MapFunction<String, Transaction>() {
                    @Override
                    public Transaction call(String value) throws Exception {


                        String[] part = value.split(delimeter);
                        Transaction trans = new Transaction();

                        int length = part.length;
                        String parts[] = new String[60];

                        if (length < 60) {

                            for (int i = 0; i < 60; i++) {
                                parts[i] = " ";
                            }

                            for (int j = 0; j < length; j++) {
                                parts[j] = part[j];
                            }
                        } else {
                            parts = part;
                        }


                        trans.setId(parts[0]);
                        trans.setTime_stamp(parts[1]);
                        trans.setSp_id(parts[2]);
                        trans.setService_provider(parts[3]);
                        trans.setApp_id(parts[4]);
                        trans.setApp_name(parts[5]);
                        trans.setState_app(parts[6]);
                        trans.setSource_entity_address(parts[7]);
                        trans.setSource_entity_masked(parts[8]);
                        trans.setChannel_type(parts[9]);

                        trans.setSource_protocol(parts[10]);
                        trans.setDest_address(parts[11]);
                        trans.setDest_masked(parts[12]);
                        trans.setDest_channel_type(parts[13]);
                        trans.setDest_protocol(parts[14]);
                        trans.setTravel_direction(parts[15]);
                        trans.setNcs(parts[16]);
                        trans.setBilling(parts[17]);
                        trans.setPart_entity_type(parts[18]);
                        trans.setCharge_amount(parts[19]);

                        trans.setCurrency(parts[20]);
                        trans.setExchange_rates(parts[21]);
                        trans.setCharging_service_code(parts[22]);
                        trans.setMsisdn(parts[23]);
                        trans.setMasked_msisdn(parts[24]);
                        trans.setBilling_event(parts[25]);
                        trans.setResponse_code(parts[26]);
                        trans.setResponse_desc(parts[27]);
                        trans.setTransaction_state(parts[28]);
                        trans.setTransaction_keyword(parts[29]);

                        trans.setCol_31(parts[30]);
                        trans.setCol_32(parts[31]);
                        trans.setCol_33(parts[32]);
                        trans.setCol_34(parts[33]);
                        trans.setCol_35(parts[34]);
                        trans.setCol_36(parts[35]);
                        trans.setCol_37(parts[36]);
                        trans.setCol_38(parts[37]);
                        trans.setCol_39(parts[38]);
                        trans.setCol_40(parts[39]);

                        trans.setCol_41(parts[40]);
                        trans.setCol_42(parts[41]);
                        trans.setCol_43(parts[42]);
                        trans.setCol_44(parts[43]);
                        trans.setCol_45(parts[44]);
                        trans.setCol_46(parts[45]);
                        trans.setCol_47(parts[46]);
                        trans.setCol_48(parts[47]);
                        trans.setCol_49(parts[48]);
                        trans.setCol_50(parts[49]);

                        trans.setCol_51(parts[50]);
                        trans.setCol_52(parts[51]);
                        trans.setCol_53(parts[52]);
                        trans.setCol_54(parts[53]);
                        trans.setCol_55(parts[54]);
                        trans.setCol_56(parts[55]);
                        trans.setCol_57(parts[56]);
                        trans.setCol_58(parts[57]);
                        trans.setCol_59(parts[58]);
                        trans.setCol_60(parts[59]);

                        System.out.println(parts[0]);
                        return trans;
                    }

                }, Encoders.bean(Transaction.class));

        return tranStream;


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
