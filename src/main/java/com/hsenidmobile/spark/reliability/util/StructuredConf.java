package com.hsenidmobile.spark.reliability.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.Serializable;

public class StructuredConf {

    private Config conf = ConfigFactory.parseResources("TypeSafeConfig.conf");

    private String master = conf.getString("application.master");
    private int shuffle = conf.getInt("application.shuffle");
    private String format = conf.getString("structured.format");
    private String parquetPath = conf.getString("structured.parquet.parquetPath");
    private String checkpointPath = conf.getString("structured.parquet.checkpointPath");
    private String bootstrap = conf.getString("kafka.bootstrapServers");
    private String topic = conf.getString("kafka.topicSet");


    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public int getShuffle() {
        return shuffle;
    }

    public void setShuffle(int shuffle) {
        this.shuffle = shuffle;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getParquetPath() {
        return parquetPath;
    }

    public void setParquetPath(String parquetPath) {
        this.parquetPath = parquetPath;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
