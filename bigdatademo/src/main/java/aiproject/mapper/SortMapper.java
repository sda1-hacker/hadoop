package aiproject.mapper;

import aiproject.writable.CustomDoubleWritable;
import aiproject.writable.IntDoublePairWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 输入        密度距离的乘积     id,距离,密度
public class SortMapper extends Mapper<LongWritable, Text, CustomDoubleWritable,IntDoublePairWritable> {
    private CustomDoubleWritable mul = new CustomDoubleWritable();
    private IntDoublePairWritable res_val = new IntDoublePairWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] temp = value.toString().split("\t");
        mul.set(Double.valueOf(temp[0]));//将DoubleWritable转为CustomDoubleWritable，用于排序
        //CustomDoubleWritable实现了WritableComparable接口，可以按照其实际数值大小来排序

        String[] id_dis_des = temp[1].split(",");
        res_val.setFirst(Double.valueOf(id_dis_des[2]));  // 密度
        res_val.setSecond(Double.valueOf(id_dis_des[1])); // 距离
        res_val.setThird(Integer.valueOf(id_dis_des[0]));  // id
        context.write(mul, res_val);
    }
}

