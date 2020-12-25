package com.hadoop.demo.wordcount;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{


    /**
     *
     * @param key           文件行偏移量,  行号
     * @param value         当前这一行的值， String
     * @param context       上下文， context.write(k, v); 将maptask的结果存储在 本地磁盘中
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineString = value.toString().trim();

        String[] tmpString = lineString.split(" ");

        for (String tmp : tmpString) {
            context.write(new Text(tmp), new IntWritable(1));
        }

    }
}
