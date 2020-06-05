package aiproject.mapper;

import aiproject.utils.DistanceUtil;
import aiproject.utils.StringUtil;
import aiproject.writable.IntPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 用户距离计算
 * 输入：key->行号    value->id  type    [a,b,c,d]
 * 输出：<距离，(用户i, 用户j)>
 */
public class DistanceMapper extends Mapper<LongWritable, Text, DoubleWritable, IntPairWritable> {

    private Path inputPath;
    private DoubleWritable res_key = new DoubleWritable();
    private IntPairWritable res_value = new IntPairWritable();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        inputPath = new Path(context.getConfiguration().get("inputPath"));  // job定义的路径
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem hdfs = FileSystem.get(conf);

        // 获取到map读取的文件的key 和 value
        // id   type    a,b,c,d
        String[] map_read = StringUtil.getFromDoubleArrIntWritable(value.toString());
        int id_i = Integer.valueOf(map_read[0]);
        double[] vector_i = StringUtil.getFromVector(map_read[2]);

        FileStatus[] fs = inputPath.getFileSystem(conf).listStatus(inputPath); // 输入目录下的所有文件
        for(FileStatus f : fs){
            if(!f.toString().contains("part")){     // 去重之后的文件名如：part-r-00000，还有_SUCCESS , 只处理包含part的文件
                continue;
            }
            // 获取到文件列表中一个具体文件的 key 和value
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(f.getPath())));
            String line;
            line = br.readLine();
            while (line != null) { // 遍历当前文件
                String[] file_read = StringUtil.getFromDoubleArrIntWritable(line);
                int id_j = Integer.valueOf(file_read[0]);
                double[] vector_j = StringUtil.getFromVector(file_read[2]);
                // 为了避免重复计算（i与j的距离和j与i的距离计算一个即可），
                //对于特定的用户i（i是用户id，也就是id_i），只有当遍历到的用户id>i时（也就是id_i<id_j），才计算两者距离
                if(id_i < id_j){
                    double dis= DistanceUtil.getDistance(vector_i, vector_j);
                    res_key.set(dis);
                    res_value.setValue(id_i, id_j);
                    context.write(res_key, res_value);
                }
                line = br.readLine();
            }
            br.close();

        }
    }
}






















