package aiproject.reducer;

import aiproject.writable.DoubleArrIntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 输出：  id  type    [a,b,c,d]
// id， 类型， 用户向量
public class DataFormatReducer extends Reducer<IntWritable, DoubleArrIntWritable, IntWritable, DoubleArrIntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<DoubleArrIntWritable> values, Context context) throws IOException, InterruptedException {
        for(DoubleArrIntWritable val : values){
            ;context.write(key, val);
        }
    }
}
