package com.ibeifeng.sparkproject.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF2;

public class GetJsonObjectUDF  implements UDF2<String,String,String> {
    @Override
    public String call(String json, String field) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(json);
        String  string = jsonObject.getString(field);
        return string;
    }
}
