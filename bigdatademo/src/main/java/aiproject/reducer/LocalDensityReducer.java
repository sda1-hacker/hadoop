package aiproject.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * 输出
 * 向量i编号，1
 * 向量j编号，1
 * 向量编号 局部密度
 */
public class LocalDensityReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    private DoubleWritable sumAll = new DoubleWritable();
    @Override
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context cxt)
            throws IOException,InterruptedException{
        double sum = 0;
        for(DoubleWritable v:values){
            sum += v.get();//每遍历一次加1
        }
        sumAll.set(sum);// 统计总数
        cxt.write(key, sumAll);
    }

}
