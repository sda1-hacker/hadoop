package com.hadoop.demo.sort2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Sort2Mapper extends Mapper<LongWritable, Text, Sort2Writable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString().trim();

        // name, viewNum, likeNum
        String[] vals = val.split(",");

        Sort2Writable sort2Writable = new Sort2Writable();
        sort2Writable.setViewNum(Integer.parseInt(vals[1]));
        sort2Writable.setLikeNum(Integer.parseInt(vals[2]));

        context.write(sort2Writable, new Text(vals[0]));

    }
}
