package com.hsenidmobile.spark.reliability.scheduler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TimerTask;

public class WriteFile extends TimerTask{

    private int count;

    @Override
    public void run() {

        try {

            count ++;
            String content = "0171012180029044"+ count +"|2017-10-13 18:00:29 117|SPP_000938|hsenid217@gmail.com|APP_005541|Winter|live|" +
                    "94753456889||sms|smpp|||||mo|subscription|unknown||||||||registration|S1000|Request was successfully processed.|" +
                    "success|winter|77000|airtel||percentageFromMonthlyRevenue|70||||S1000|Success|||0|||||||||||daily|||||||" + "\n";

            //String content = "" + count;
            File file = new File("/home/cloudera/Downloads/rasika/sdp-log1/created");

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fileWriter = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(content);
            bufferedWriter.close();

            System.out.println("New line added : " + count);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
