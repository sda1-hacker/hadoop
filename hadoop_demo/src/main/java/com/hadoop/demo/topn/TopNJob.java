package com.hadoop.demo.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TopNJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.157.129:9000");

        FileSystem fs = FileSystem.newInstance(configuration); // hdfs 文件系统

        Job topNJob = Job.getInstance(configuration);
        topNJob.setJarByClass(TopNJob.class);

        topNJob.setMapperClass(TopNMapper.class);
        topNJob.setInputFormatClass(TextInputFormat.class);
        topNJob.setMapOutputKeyClass(TopNWritable.class);
        topNJob.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(topNJob, new Path("/input/TopN.txt"));
        FileInputFormat.addInputPath(topNJob, new Path("/input/TopM.txt"));

        topNJob.setReducerClass(TopNReducer.class);
        topNJob.setOutputFormatClass(TextOutputFormat.class);
        topNJob.setOutputKeyClass(Text.class);
        topNJob.setOutputValueClass(IntWritable.class);

        Path resPath = new Path("/output");

        if (fs.exists(resPath)) {
            fs.delete(resPath, true);
        }

        FileOutputFormat.setOutputPath(topNJob, resPath);

        boolean res = topNJob.waitForCompletion(true);
        System.out.println(res);
    }
}
