package aiproject.driver;

import aiproject.mapper.SortMapper;
import aiproject.reducer.SortReducer;
import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.Url;
import aiproject.writable.CustomDoubleWritable;
import aiproject.writable.IntDoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// 第六步，排序
public class SortDriver {

    public static void main(String[] agrs) throws Exception {
        Configuration conf = HadoopConfigurationUtil.getConfig();

        // 最小距离保存的路径， 排序文件后保存的路径
        String[] args1 = {Url.DELTADISTANCEOUTPUT, Url.SORTOUTPUT};
        String[] otherArgs = new GenericOptionsParser(conf, args1).getRemainingArgs();//初始化命令参数

        Job job = Job.getInstance(conf, "sort the input:" + otherArgs[0]);

        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        // job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(CustomDoubleWritable.class);
        job.setOutputValueClass(IntDoublePairWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);

    }

}
