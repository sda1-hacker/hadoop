package com.hadoop.demo.partition;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PartitionReducer extends Reducer<PartitionWritable, Text, Text, Text> {

    @Override
    protected void reduce(PartitionWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder resVal = new StringBuilder(key.getSubject()).append("\t").append(key.getScore());
        for (Text val : values) {
            context.write(val, new Text(resVal.toString()));
        }

    }
}
