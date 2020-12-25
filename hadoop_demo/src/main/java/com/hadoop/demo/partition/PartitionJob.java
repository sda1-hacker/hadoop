package com.hadoop.demo.partition;

import com.hadoop.demo.flow.FlowJob;
import com.hadoop.demo.flow.FlowMapper;
import com.hadoop.demo.flow.FlowReducer;
import com.hadoop.demo.flow.FlowWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PartitionJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.157.129:9000");

        FileSystem fs = FileSystem.newInstance(configuration); // hdfs 文件系统

        Job job = Job.getInstance(configuration);
        job.setJarByClass(PartitionJob.class);   // 设置启动类

        // 设置reduceTask个数, 如果为0, 则表示不执行reduceTask
        // 启动多个reduceTask 会执行分区操作，默认使用HashPartitioner.class
        job.setNumReduceTasks(4);
        // 使用自定义分区
        job.setPartitionerClass(SubjectPartitioner.class);

        job.setMapperClass(PartitionMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(PartitionWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/input/subject.txt"));

        job.setReducerClass(PartitionReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path resPath = new Path("/output");

        if (fs.exists(resPath)) {
            fs.delete(resPath, true);
        }

        FileOutputFormat.setOutputPath(job, resPath);

        boolean res = job.waitForCompletion(true);  // 开启监控并且打印日志
        System.out.println(res);
    }
}
