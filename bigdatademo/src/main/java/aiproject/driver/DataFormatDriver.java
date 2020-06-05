package aiproject.driver;

import aiproject.mapper.DataFormatMapper;
import aiproject.mapper.OriginDataDistinctMapper;
import aiproject.reducer.DataFormatReducer;
import aiproject.reducer.OriginDataDistinctReducer;
import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.Url;
import aiproject.writable.DoubleArrIntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

// 第二步骤，数据格式化
// id,<type,用户的有效向量 >
public class DataFormatDriver {
    public static void main(String[] args) throws Exception{
        Configuration conf = HadoopConfigurationUtil.getConfig();

        Job job = Job.getInstance(conf);

        job.setJarByClass(DataFormatDriver.class);

        // job运行的mapper类，mapper输出的值类型
        job.setMapperClass(DataFormatMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrIntWritable.class);

        // job运行的reducer类，reducer输出的值的类型
        job.setReducerClass(DataFormatReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleArrIntWritable.class);


        // 输入输出目录
        FileInputFormat.setInputPaths(job, new Path(Url.OUTPUT_XML_PATH1));
        FileOutputFormat.setOutputPath(job, new Path(Url.OUTPUT_XML_PATH));

        // 提交任务，显示日志
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }
}
