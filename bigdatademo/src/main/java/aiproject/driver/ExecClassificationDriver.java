package aiproject.driver;

import aiproject.filter.ClusterCounter;
import aiproject.mapper.ExecClassificationMapper;
import aiproject.utils.*;
import aiproject.writable.DoubleArrIntWritable;
import aiproject.writable.IntPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Map;
// 第九步，执行分类
public class ExecClassificationDriver {

    // args[0]  --> bestDC   //距离阀值
    public static void main(String[] args) throws Exception{

        Configuration conf = HadoopConfigurationUtil.getConfig();
        Job job =  Job.getInstance(conf);   // 任务

        String delta = "0.0";//距离阀值

        String k = args[0];//聚类中心数
        // String delta = args[1]; //距离阀值

        Map<String, String> vectorsMap= SeqFileUtil.readCenterFile(Url.CENTER_OUTPUT, Integer.parseInt(k));
        double[][] vectors = CentIdUtil.getCenterVector(vectorsMap);
        double[] distances= DistanceUtil.getDistances(vectors);//获取两两聚类中心向量的距离，并按照从小到大排序

        int iter_i = 0;
        double tmpDelta = 0;
        int kInt = Integer.parseInt(k);

        // 这里不使用传入进来的阈值     通过计算得到
        do {
            if(iter_i >= distances.length){//distances.length=K*(K-1)/2
                // 当读取到最后一个向量距离时，使用如下方式计算阀值
                tmpDelta = Double.parseDouble(delta);
                while(kInt-- > 0){// 超过k次后就不再增大
                    tmpDelta *= 2;// 每次翻倍
                }
                delta = String.valueOf(tmpDelta);//最终的阀值
            }else{
                //距离数组未越界时，直接取读取距离的一半为阀值
                delta = String.valueOf(distances[iter_i]/2);
            }
            System.out.println("距离阈值" + iter_i + ":  " + delta);
            String[] args1={
                    Url.OUTPUT_XML_PATH,
                    Url.CLUSTERED + (iter_i+1),//output
                    delta
            };//初始化MapReduce运行参数
            try{
                exec(job, conf, args1);
            }catch(Exception e){
                e.printStackTrace();
            }
            iter_i++;

        }while(shouldRunNextIter(job)); //通过方法shouldRunNextIter()来判断是否终止循环

    }


    public static void exec(Job job, Configuration conf, String[] args) throws Exception{
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//初始化命令行参数
        conf.setDouble("DC", Double.parseDouble(otherArgs[2]));//设置距离阀值

        job.setJarByClass(ExecClassificationDriver.class);
        job.setMapperClass(ExecClassificationMapper.class);

        // map输出
        job.setMapOutputKeyClass(IntWritable.class   );
        job.setMapOutputValueClass(DoubleArrIntWritable.class);

        // reducer输出
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleArrIntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);
    }


    /**
     * 是否应该继续下次循环
     * 直接使用分类记录数和未分类记录数来判断
     * @throws
     * @throws IllegalArgumentException
     */
    private static boolean shouldRunNextIter(Job job) throws IOException {

        long UNCLUSTERED = job.getCounters().findCounter(ClusterCounter.UNCLUSTERED).getValue();
        long CLUSTERED = job.getCounters().findCounter(ClusterCounter.CLUSTERED).getValue();

        if(UNCLUSTERED == 0 || CLUSTERED == 0){//MapReduce任务运行时会修改UNCLUSTERED和CLUSTERED的值
            //当两者其中一个为0时，则表示所有数据已经聚类完成，则结束循环
            return false;
        }
        return true;

    }

}
