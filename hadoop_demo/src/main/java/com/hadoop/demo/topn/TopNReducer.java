package com.hadoop.demo.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNReducer extends Reducer<TopNWritable, Text, Text, IntWritable> {

    private Integer count = 0; // 计数器

    // 到reduce中的数据是已经根据key的大小来排过序的, 取top3
    @Override
    protected void reduce(TopNWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (count < 3) {
            for (Text name : values) {
                context.write(name, new IntWritable(key.getLikeNum()));
            }
        }
        count++;

    }
}
