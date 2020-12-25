package com.hadoop.demo.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 自定义数据类型
@NoArgsConstructor
@AllArgsConstructor
@Data
public class FlowWritable implements Writable{  // 实现Writable接口实现hadoop序列化

    // 需要把自定义序列化的所有属性都赋值，否则报错
    private Long up;    // 上传
    private Long down;  // 下载
    private Long all;

    // 序列化  jvm --> 磁盘
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(up);
        dataOutput.writeLong(down);
        dataOutput.writeLong(all);
    }

    // 反序列化     磁盘 --> jvm      -- 反序列化的顺序和序列化的顺序是一致的
    public void readFields(DataInput dataInput) throws IOException {
        this.up = dataInput.readLong();
        this.down = dataInput.readLong();
        this.all = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "FlowWritable{" +
                "up=" + up +
                ", down=" + down +
                ", all=" + all +
                '}';
    }
}
