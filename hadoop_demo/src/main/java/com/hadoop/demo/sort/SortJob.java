package com.hadoop.demo.sort;

import com.hadoop.demo.flow.FlowWritable;
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

public class SortJob {


    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.157.129:9000");

        FileSystem fs = FileSystem.newInstance(configuration); // hdfs 文件系统

        Job sortJob = Job.getInstance(configuration);
        sortJob.setJarByClass(SortJob.class);   // 设置启动类

        // 设置reduceTask个数, 如果为0, 则表示不执行reduceTask
        sortJob.setNumReduceTasks(1);

        sortJob.setMapperClass(SortMapper.class);
        sortJob.setMapOutputKeyClass(SortWritable.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(sortJob, new Path("/input/sort.log"));

        sortJob.setReducerClass(SortReducer.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(IntWritable.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        Path resPath = new Path("/output");

        if (fs.exists(resPath)) {
            fs.delete(resPath, true);
        }

        FileOutputFormat.setOutputPath(sortJob, resPath);

        boolean res = sortJob.waitForCompletion(true);
        System.out.println(res);
    }

}
