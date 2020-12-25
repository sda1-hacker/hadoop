package com.hadoop.demo.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class FlowJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.157.129:9000");

        FileSystem fs = FileSystem.newInstance(configuration); // hdfs 文件系统

        Job flowJob = Job.getInstance(configuration);
        flowJob.setJarByClass(FlowJob.class);   // 设置启动类

        // 设置reduceTask个数, 如果为0, 则表示不执行reduceTask
        flowJob.setNumReduceTasks(1);

        flowJob.setMapperClass(FlowMapper.class);
        flowJob.setInputFormatClass(TextInputFormat.class);
        flowJob.setMapOutputKeyClass(Text.class);
        flowJob.setOutputValueClass(FlowWritable.class);

        FileInputFormat.addInputPath(flowJob, new Path("/input/phone.log"));

        flowJob.setReducerClass(FlowReducer.class);
        flowJob.setOutputFormatClass(TextOutputFormat.class);
        flowJob.setOutputKeyClass(Text.class);
        flowJob.setOutputValueClass(FlowWritable.class);

        Path resPath = new Path("/output");

        if (fs.exists(resPath)) {
            fs.delete(resPath, true);
        }

        FileOutputFormat.setOutputPath(flowJob, resPath);

        boolean res = flowJob.waitForCompletion(true);
        System.out.println(res);

    }
}
