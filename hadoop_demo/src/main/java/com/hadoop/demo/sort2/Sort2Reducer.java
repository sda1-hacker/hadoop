package com.hadoop.demo.sort2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Sort2Reducer extends Reducer<Sort2Writable, Text, Text, Sort2Writable> {

    @Override
    protected void reduce(Sort2Writable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text name : values) {
            context.write(name, key);
        }

    }
}
