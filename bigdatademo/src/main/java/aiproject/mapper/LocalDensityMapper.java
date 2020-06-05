package aiproject.mapper;

import aiproject.utils.StringUtil;
import aiproject.writable.IntPairWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 局部密度
 * 输入为<距离d_ij,<向量i编号，向量j编号>
 * 根据距离dc阈值判断距离d_ij是否小于dc，符合要求则
 * 输出
 * 向量i编号，1
 * 向量j编号，1
 */
public class LocalDensityMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{

    private double dc;
    private IntWritable vectorId = new IntWritable();
    private DoubleWritable one = new DoubleWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dc = context.getConfiguration().getDouble("DC", 0);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 距离   id_i,id_j
        String[] temp = StringUtil.getFromDoubleAndIntPairWritable(value.toString());
        // 距离
        double distance= Double.valueOf(temp[0]);
        // id_i,id_j
        int[] ids = StringUtil.getFromUserIdPair(temp[1]);

        // one.set(Math.pow(Math.E, -(distance/dc)*(distance/dc)));//gaussian计算局部密度

        if(distance<dc){//两者距离小于阀值，则输出
            vectorId.set(ids[0]);
            context.write(vectorId, one);
            vectorId.set(ids[1]);
            context.write(vectorId, one);
        }

    }
}
