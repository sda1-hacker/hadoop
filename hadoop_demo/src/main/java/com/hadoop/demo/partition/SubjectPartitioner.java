package com.hadoop.demo.partition;

import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// 自定义分区类
// 默认使用 HashPartitioner.class
// 泛型： mapper中输出 key, value的泛型
@NoArgsConstructor
public class SubjectPartitioner extends Partitioner<PartitionWritable, Text> {

    public int getPartition(PartitionWritable partitionWritable, Text text, int i) {

        String subject = partitionWritable.getSubject();

        if ("数学".equals(subject)) {
            return 0;
        } else if ("语文".equals(subject)) {
            return 1;
        } else if ("英语".equals(subject)) {
            return 2;
        }

        return 3;
    }
}
