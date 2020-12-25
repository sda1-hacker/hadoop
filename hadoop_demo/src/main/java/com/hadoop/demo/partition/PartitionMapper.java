package com.hadoop.demo.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable, Text, PartitionWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString().trim();

        String[] vals = val.split("\t");

        Text resVal = new Text(vals[0]);

        PartitionWritable resKey = new PartitionWritable(vals[1], Integer.parseInt(vals[2]));

        context.write(resKey, resVal);

    }
}
