package com.hadoop.demo.topn;

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
public class TopNWritable implements WritableComparable<TopNWritable> {

    private Integer likeNum;

    public int compareTo(TopNWritable o) {
        TopNWritable that = o;
        return that.likeNum - this.likeNum; // 降序
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(likeNum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.likeNum = dataInput.readInt();
    }
}

