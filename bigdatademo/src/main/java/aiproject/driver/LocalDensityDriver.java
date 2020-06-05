package aiproject.driver;

import aiproject.mapper.LocalDensityMapper;
import aiproject.reducer.LocalDensityReducer;
import aiproject.utils.DCUtil;
import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.Url;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;

// 第四步，局部密度
public class LocalDensityDriver {

    // 距离文件总数  --  可以通过hdfs命令查看， 然后传递给这个函数
    // hdfs dfs -cat /aiproject/dis/output/part-r-00000 | wc -l
    public static void main(String[] args) throws Exception {

        Configuration conf = HadoopConfigurationUtil.getConfig();

        String[] args1 = {Url.OUTPUT_DIS_PATH, Url.OUTPUT_LOCAL_DENSITY_PATH};
        String[] otherArgs = new GenericOptionsParser(conf, args1).getRemainingArgs();//初始化命令行参数

        // 距离文件总数  --  可以通过hdfs命令查看， 然后传递给这个函数
        // hdfs dfs -cat /aiproject/dis/output/part-r-00000 | wc -l
        long INPUT_RECORDS = Long.valueOf(args[0]);

        double DC = DCUtil.getBestDC(INPUT_RECORDS);
        conf.setDouble("DC", DC);//设置距离阀值

        Job job = Job.getInstance(conf, "Count the near neighours with a given dc(distance)");

        // job启动类
        job.setJarByClass(LocalDensityDriver.class);
        job.setMapperClass(LocalDensityMapper.class);
        job.setCombinerClass(LocalDensityReducer.class);
        job.setReducerClass(LocalDensityReducer.class);

        // map输出
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // reduce输出
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // job输入，输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

        boolean res = job.waitForCompletion(true);

        System.out.println("DC: " + DC);    // 29.427877939124322


        System.exit(res?0:1);


    }
}
