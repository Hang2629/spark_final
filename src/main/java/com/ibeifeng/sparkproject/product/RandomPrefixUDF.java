package com.ibeifeng.sparkproject.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

public class RandomPrefixUDF implements UDF2<String,Integer,String> {
    @Override
    public String call(String val, Integer num) throws Exception {
        Random random = new Random();
        int i = random.nextInt(10);
        return i + "_"+ val;
    }
}
