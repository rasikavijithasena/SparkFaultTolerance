package com.hsenidmobile.spark.reliability.util;

/**
 * Created by cloudera on 11/23/17.
 */

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class InputFieldValues implements Serializable{

    Config conf = ConfigFactory.parseResources("TypeSafeConfig.conf");

    private List<String> selectedFields = conf.getStringList("input.selectedFields");
    private String delimeter = conf.getString("input.delimeter");
    private int fieldCount = conf.getInt("input.fieldCount");
    private String directory = conf.getString("input.directory");


    public List<String> getSelectedFields() {
        return selectedFields;
    }

    public int getNumberOfSelectedFields() {
        return selectedFields.size();
    }

    public int getFieldNumber(int listNumber) {
        String field = selectedFields.get(listNumber);
        int fieldNumber = conf.getInt("input."+field+".fieldNumber");
        return fieldNumber;
    }

    public List<Integer> getSubString(int listNumber) {
        String field = selectedFields.get(listNumber);
        Boolean substring = conf.getBoolean("input."+field+".substring");

        List<Integer> substringValue = new ArrayList<>();
        if(substring == true) {
            substringValue = conf.getIntList("input."+field+".substringValue");

        } else {

        }
        return substringValue;
    }

    public String getDelimeter() {
        return "\\" +delimeter;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public String getDirectory() {
        return directory;
    }
}
