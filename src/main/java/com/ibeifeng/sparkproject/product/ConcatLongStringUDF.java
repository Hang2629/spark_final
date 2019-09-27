package com.ibeifeng.sparkproject.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 *
 */
public class ConcatLongStringUDF implements UDF3<Long,String,String,String> {

    @Override
    public String call(Long aLong, String s, String split) throws Exception {
        return String.valueOf(aLong + split  + s);
    }
}
