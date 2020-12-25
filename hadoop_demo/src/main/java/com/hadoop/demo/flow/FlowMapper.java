package com.hadoop.demo.flow;

import net.minidev.json.JSONUtil;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString().trim();

        String[] vals = val.split("\t");

        FlowWritable fw = new FlowWritable();
        fw.setUp(Long.parseLong(vals[6]));
        fw.setDown(Long.parseLong(vals[7]));
        fw.setAll(0l);  // 需要把自定义序列化的所有属性都赋值，否则报错

        // 计数器 -- 统计个数， 最后会输出到控制台
        // 每执行一次map函数，+1  组名, 计数器名
        context.getCounter("flowMapper组", "map计数器").increment(1l);

        context.write(new Text(vals[1]), fw);

    }
}
