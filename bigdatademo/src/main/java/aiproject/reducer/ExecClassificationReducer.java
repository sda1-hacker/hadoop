package aiproject.reducer;

import aiproject.writable.DoubleArrIntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ExecClassificationReducer extends Reducer<IntWritable, DoubleArrIntWritable, IntWritable, DoubleArrIntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<DoubleArrIntWritable> values, Context context) throws IOException, InterruptedException {
        for(DoubleArrIntWritable val : values){
            context.write(key, val);
        }
    }
}
