package com.hadoop.demo.partition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PartitionWritable implements WritableComparable<PartitionWritable> {

    private String subject;
    private Integer score;

    // 如果改方法返回0, 则认为是相同的key, 数据可能会合并, 因此需要将多个字段都进行简单的排序
    public int compareTo(PartitionWritable o) {
        PartitionWritable that = o;

        if (this.score != that.score) {
            return that.score - this.score;
        } else {
            return this.subject.compareTo(that.subject);
        }

    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(subject);
        dataOutput.writeInt(score);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.subject = dataInput.readUTF();
        this.score = dataInput.readInt();
    }
}
