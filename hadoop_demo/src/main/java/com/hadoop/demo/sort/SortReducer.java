package com.hadoop.demo.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(SortWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text val : values) {
            context.write(val, new IntWritable(key.getNums()));
        }

    }
}
