package com.hadoop.demo.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString().trim();

        String[] vals = val.split(",");

        Integer viewNum = Integer.parseInt(vals[1]);
        String name = vals[0];

        // System.out.println(viewNum + "--" + name);

        context.write(new SortWritable(viewNum), new Text(name));

    }
}
