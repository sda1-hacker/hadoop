package aiproject.mapper;

import aiproject.filter.ClusterCounter;
import aiproject.utils.*;
import aiproject.writable.DoubleArrIntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

// 输入： id   type [a,b,c,d]
public class ExecClassificationMapper extends Mapper<LongWritable, Text, IntWritable, DoubleArrIntWritable> {

    private double dc =0.0;
    private int k; // 聚类中心个数
    private DoubleArrIntWritable typeDoubleArr = new DoubleArrIntWritable();
    private IntWritable vectorI = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dc = context.getConfiguration().getDouble("DC", Double.MAX_VALUE);
        context.getCounter(ClusterCounter.CLUSTERED).increment(0);
        context.getCounter(ClusterCounter.UNCLUSTERED).increment(0);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // id  type    [a,b,c,d]
        String[] temp = StringUtil.getFromDoubleArrIntWritable(value.toString());
        double[] inputI= StringUtil.getFromVector(temp[2]);

        // hdfs
        Configuration conf = HadoopConfigurationUtil.getConfig();
        FileSystem hdfs = FileSystem.get(conf);

        double smallDistance = Double.MAX_VALUE;
        int smallDistanceType = -1;
        double distance;


        // 聚类中心文件
        // 141132	3	397.0,6.0,1.0,11.0
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(Url.CENTER_OUTPUT))));
        String line;
        line = br.readLine(); //  id   type     a,b,c,d
        while (line != null) { // 遍历当前文件
            String[] temp_s = line.split("\t");;
            double[] vector = StringUtil.getFromVector(temp_s[2]);
            distance = DistanceUtil.getDistance(inputI, vector);//获取距离
            if(distance <= dc){// 距离小于阀值，进行处理
                // 这里只要找到离该点最近的点并且其distance<=dc 即可，把这个点的type赋值给当前值即可
                if(distance < smallDistance){
                    smallDistance = distance;
                    smallDistanceType = Integer.valueOf(temp_s[1]);//找到符合要求的类别
                }
            }
            line = br.readLine();
        }
        br.close();

        if(smallDistanceType!=-1){ // 已分类
            context.getCounter(ClusterCounter.CLUSTERED).increment(1);//分类计数加1
            vectorI.set(Integer.valueOf(temp[0]));// 用户id
            typeDoubleArr.setValue(inputI, smallDistanceType); // type  [a,b,c,d]
            context.write(vectorI, typeDoubleArr);
        }
        else{
            context.getCounter(ClusterCounter.UNCLUSTERED).increment(1);//未分类计数加1
        }
    }

}
