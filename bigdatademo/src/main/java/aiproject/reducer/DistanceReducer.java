package aiproject.reducer;

import aiproject.writable.IntPairWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 用户距离计算
 * 输出：<距离，(用户i, 用户j)>
 */
public class DistanceReducer extends Reducer<DoubleWritable, IntPairWritable, DoubleWritable, IntPairWritable> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
        for(IntPairWritable v : values){
            context.write(key, v);
        }
    }
}
