package com.hadoop.demo.topn;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, TopNWritable, Text> {

    // 会按照key进行排序， 默认升序
    private TreeMap<Integer, String> treeMap;

    private Integer topN = 3;

    // 在执行mapTask执行之前调用， 只会调用一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 降序
        Comparator comparator = new Comparator<Integer>(){
            public int compare(Integer o1, Integer o2) {
                return o2 - o1;
            }
        };

        treeMap = Maps.newTreeMap(comparator);
    }

    // 在mapTask执行完毕之后调用， 只会调用一次
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, String> entry : treeMap.entrySet()) {
            context.write(new TopNWritable(entry.getKey()), new Text(entry.getValue()));
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString().trim();

        String[] vals = val.split(",");

        treeMap.put(Integer.parseInt(vals[1]), vals[0]);

        if (treeMap.size() > topN) {
            Integer mapKey = treeMap.lastKey(); // 默认是升序 -- 传递了比较器，使用降序排列
            treeMap.remove(mapKey); // 移除最小的
        }

    }
}
