package com.hadoop.demo.sort2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Sort2Job {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.157.129:9000");

        FileSystem fs = FileSystem.newInstance(configuration); // hdfs 文件系统

        Job sort2Job = Job.getInstance(configuration);
        sort2Job.setJarByClass(Sort2Job.class);   // 设置启动类

        // 设置reduceTask个数, 如果为0, 则表示不执行reduceTask
        sort2Job.setNumReduceTasks(1);

        sort2Job.setMapperClass(Sort2Mapper.class);
        sort2Job.setMapOutputKeyClass(Sort2Writable.class);
        sort2Job.setMapOutputValueClass(Text.class);
        sort2Job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(sort2Job, new Path("/input/sort2.log"));
        // 输入文件可以有多个, 多次调用addInputPath就可以了
        // FileInputFormat.addInputPath(sort2Job, new Path("/input/sort3.log"));

        // 或者直接给某个目录
        // FileInputFormat.addInputPath(sort2Job, new Path("/input/"));

        sort2Job.setReducerClass(Sort2Reducer.class);
        sort2Job.setOutputKeyClass(Text.class);
        sort2Job.setOutputValueClass(Sort2Writable.class);
        sort2Job.setOutputFormatClass(TextOutputFormat.class);

        Path resPath = new Path("/output");

        if (fs.exists(resPath)) {
            fs.delete(resPath, true);
        }

        FileOutputFormat.setOutputPath(sort2Job, resPath);

        boolean res = sort2Job.waitForCompletion(true);
        System.out.println(res);
    }

}
