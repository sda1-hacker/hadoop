package com.hadoop.demo.sort;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// WritableComparable 同时支持序列化和排序顺序 -- 泛型是当前的类
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SortWritable implements WritableComparable<SortWritable> {

    private Integer nums;

    // jvm -> 磁盘
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(nums);
    }

    // 磁盘 -> JVM
    public void readFields(DataInput dataInput) throws IOException {
        this.nums = dataInput.readInt();
    }


    public int compareTo(SortWritable o) {
        SortWritable that = o;
        return this.nums - that.nums; // 升序

        // return that.nums - this.nums; // 降序
    }
}
