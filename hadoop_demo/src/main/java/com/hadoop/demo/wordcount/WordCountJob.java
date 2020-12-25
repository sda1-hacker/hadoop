package com.hadoop.demo.wordcount;

import com.hadoop.demo.wordcount.WordCountMapper;
import com.hadoop.demo.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCountJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // hdfs
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.157.129:9000");

        // 创建任务对象
        Job wordCountJob = Job.getInstance(configuration);
        wordCountJob.setJarByClass(WordCountJob.class); // 设置启动类

        // 组装MapTask相关信息
        // 使用的mapper类
        wordCountJob.setMapperClass(WordCountMapper.class);
        // 使用哪个输入工具类    TextInputFormat -- 逐行读取
        wordCountJob.setInputFormatClass(TextInputFormat.class);
        // map函数输出的 k, v 的类型
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);

        // FileSystem fs = FileSystem.newInstance(configuration);
        // fs.exists(new Path(""));

        // map读取的文件
        FileInputFormat.addInputPath(wordCountJob, new Path("/input/wordCount.txt"));


        // 组装ReduceTask相关的信息
        // 使用的reducer类
        wordCountJob.setReducerClass(WordCountReducer.class);
        // reduce使用的输出类     TextOutputFormat -- 将文件输出到hdfs中
        wordCountJob.setOutputFormatClass(TextOutputFormat.class);
        // reduce函数输出的 k, v的类型
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        // reduce输出的文件 -- 输出结果的目录不能提前创建，否则会报错
        FileOutputFormat.setOutputPath(wordCountJob, new Path("/output"));

        //
        boolean res = wordCountJob.waitForCompletion(true);
        System.out.println(res); // true 表示执行正常， false 表示执行过程中出现错误

        // 在linux上运行hadoop程序
        // hadoop jar /xx/xx/xxxxx.jar com.hadoop.demo.job.WordCountJob
        // yar jar /xx/xx/xxxxx.jar com.hadoop.demo.job.WordCountJob

    }
}
