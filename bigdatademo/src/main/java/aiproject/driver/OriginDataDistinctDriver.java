package aiproject.driver;

import aiproject.mapper.OriginDataDistinctMapper;
import aiproject.reducer.OriginDataDistinctReducer;
import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.Url;
import aiproject.writable.DoubleArrIntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

// 第一步，数据去重
public class OriginDataDistinctDriver {

    public static void main(String[] args) throws Exception{
        // 设置运行参数
        Configuration conf = HadoopConfigurationUtil.getConfig();

        Job job = Job.getInstance(conf);

        // 运行类
        job.setJarByClass(OriginDataDistinctDriver.class);

        // job运行的mapper类，mapper输出的值类型
        job.setMapperClass(OriginDataDistinctMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // job运行的reducer类，reducer输出的值的类型
        job.setReducerClass(OriginDataDistinctReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 输出序列化
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 输入输出目录
        FileInputFormat.setInputPaths(job, new Path(Url.INPUT_XML_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Url.OUTPUT_XML_PATH1));
        // SequenceFileOutputFormat.setOutputPath(job, new Path(Url.OUTPUT_XML_PATH1));

        // 提交任务，显示日志
        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);

    }
}
