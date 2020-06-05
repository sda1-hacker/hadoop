package aiproject.driver;

import aiproject.filter.FilterCounter;
import aiproject.mapper.DistanceMapper;
import aiproject.reducer.DistanceReducer;
import aiproject.utils.DistanceUtil;
import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.Url;
import aiproject.writable.IntPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// 第三步，计算距离
public class DistanceDriver {
    public static void main(String[] args) throws Exception{

        // 设置运行参数
        Configuration conf = HadoopConfigurationUtil.getConfig();

        // 计算距离的输入目录，计算距离的输出目录
        String [] paths ={Url.OUTPUT_XML_PATH, Url.OUTPUT_DIS_PATH};
        String[] otherArgs = new GenericOptionsParser(conf, paths).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: com.kang.filter.CalDistanceJob <in> <out>");
            System.exit(2);
        }
        conf.set("inputPath", otherArgs[0]);

        Job job =  Job.getInstance(conf,"from  input  :"+
                otherArgs[0]+" to "+otherArgs[1]);

        // job启动类，mapper，reducer类
        job.setJarByClass(DistanceDriver.class);
        job.setMapperClass(DistanceMapper.class);
        job.setReducerClass(DistanceReducer.class);
        job.setNumReduceTasks(1); // 一个reduce任务

        // map输出
        job.setMapOutputKeyClass(DoubleWritable.class   );
        job.setMapOutputValueClass(IntPairWritable.class);

        // reducer输出
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(IntPairWritable.class);

        // job输入，输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

        boolean res = job.waitForCompletion(true);

        // 距离文件的记录总数，用于寻找最佳DC
        long records=job.getCounters().findCounter(FilterCounter.REDUCE_COUNTER).getValue();
        DistanceUtil.INPUT_RECORDS = records;
        System.out.println(records);
        System.exit(res?0:1);
    }
}
