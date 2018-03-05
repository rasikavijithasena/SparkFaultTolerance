package com.hsenidmobile.spark.reliability.scheduler;

import java.util.Timer;

/**
 * Created by cloudera on 12/6/17.
 */
public class FileUpdateScheduler {

    public static void main(String[] args) {
        Timer timer = new Timer();
        WriteFile writeFile = new WriteFile();

        timer.scheduleAtFixedRate(writeFile, 0, 10000);
    }
}
