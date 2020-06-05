package aiproject.reducer;

import aiproject.writable.CustomDoubleWritable;
import aiproject.writable.IntDoublePairWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<CustomDoubleWritable, IntDoublePairWritable, CustomDoubleWritable,IntDoublePairWritable> {
    @Override
    protected void reduce(CustomDoubleWritable key, Iterable<IntDoublePairWritable> values, Context context) throws IOException, InterruptedException {
            for(IntDoublePairWritable v : values){
                context.write(key, v);//直接输出即可，MapReduce框架自动为我们排好序
            }
        }
}
