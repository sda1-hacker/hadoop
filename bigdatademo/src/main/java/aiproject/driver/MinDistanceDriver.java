package aiproject.driver;

import aiproject.mapper.MinDistanceMapper;
import aiproject.reducer.MinDistanceReducer;
import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.Url;
import aiproject.writable.DoublePairWritable;
import aiproject.writable.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 寻找大于自身密度的最小其他向量的距离
 * mapper输入：
 * 输入为<距离d_ij,<向量i编号，向量j编号>>
 * 把LocalDensityJob的输出
 *      i,density_i
 * 放入一个map中，用于在mapper中进行判断两个局部密度的大小以决定是否输出
 * mapper输出：
 *      i,<density_i,min_distance_j>
 *      IntWritable,DoublePairWritable
 * reducer 输出：
 *      <density_i*min_distancd_j> <density_i,min_distance_j,i>
 *      DoubleWritable,  IntDoublePairWritable
 */

// 第五步，计算最短距离
public class MinDistanceDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopConfigurationUtil.getConfig();

        String[] args1 = {Url.OUTPUT_DIS_PATH, Url.DELTADISTANCEOUTPUT};
        String[] otherArgs = new GenericOptionsParser(conf, args1).getRemainingArgs();//初始化命令行参数

        Job job =  Job.getInstance(conf,"find the nearest distance with the bigger near neighours");
        job.setJarByClass(MinDistanceDriver.class);
        job.setMapperClass(MinDistanceMapper.class);
        // job.setCombinerClass(MinDistanceReducer.class);
        job.setReducerClass(MinDistanceReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoublePairWritable.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(IntDoublePairWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);

    }
}
