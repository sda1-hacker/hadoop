package com.hadoop.demo.sort2;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class Sort2Writable implements WritableComparable<Sort2Writable> {

    private Integer viewNum;
    private Integer likeNum;


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(viewNum);
        dataOutput.writeInt(likeNum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.viewNum = dataInput.readInt();
        this.likeNum = dataInput.readInt();
    }

    // 如果第一个字段不相同则按照第一个字段排序
    // 如果第一个字段相同则按照第二个字段排序
    public int compareTo(Sort2Writable o) {
        Sort2Writable that = o;

        // 升序
        if (this.getViewNum() != that.getViewNum()) {
            return this.getViewNum() - that.getViewNum();
        } else {
            return this.getLikeNum() - that.getLikeNum();
        }
    }
}
