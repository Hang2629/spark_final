package com.ibeifeng.sparkproject.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("cityInfo",DataTypes.StringType,true)
    ));
    private StructType bufferScheme = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("bufferCityInfo",DataTypes.StringType,true)
    ));

    private DataType dataType = DataTypes.StringType;

    private boolean deterministic = true;
    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferScheme;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,"");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        String updateBuffer = buffer.getString(0);
        String cityinfo = input.getString(0);
        if(!updateBuffer.contains(cityinfo)) {
            if("".equals(updateBuffer)) {
                updateBuffer += cityinfo;
            }else {
                updateBuffer += "，" + cityinfo;
            }
        }
        buffer.update(0,updateBuffer);
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(2);

        for (String s:bufferCityInfo2.split(",")) {
            if(!bufferCityInfo1.contains(s)) {
                if("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += s;
                }else {
                    bufferCityInfo1 += "," + s;
                }
            }

        }
        buffer1.update(0,bufferCityInfo1);

    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
