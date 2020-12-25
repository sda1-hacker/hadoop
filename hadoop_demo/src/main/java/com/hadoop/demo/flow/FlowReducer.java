package com.hadoop.demo.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowWritable, Text, FlowWritable> {

    @Override
    protected void reduce(Text key, Iterable<FlowWritable> values, Context context) throws IOException, InterruptedException {

        Long allUp = 0l;
        Long allDown = 0l;
        for(FlowWritable tmp : values){
            allUp = allUp + tmp.getUp();
            allDown = allDown + tmp.getDown();
        }

        FlowWritable resFW = new FlowWritable();
        resFW.setUp(allUp);
        resFW.setDown(allDown);
        resFW.setAll(allUp + allDown);

        context.write(key, resFW);

    }
}
