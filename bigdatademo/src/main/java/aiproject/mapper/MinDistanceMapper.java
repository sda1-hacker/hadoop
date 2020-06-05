package aiproject.mapper;

import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.StringUtil;
import aiproject.utils.Url;
import aiproject.writable.DoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


// 输入  距离   type a,b,c,d
public class MinDistanceMapper extends Mapper<LongWritable, Text, IntWritable, DoublePairWritable> {

    private IntWritable vector_i= new IntWritable();
    private DoublePairWritable density_distance = new DoublePairWritable();
    private Path densityPath ;
    private Map<Integer,Double> densityMap = new HashMap<Integer,Double>();// vector_id,density
    private int max_density_vector_id = -1;// 最大的局部密度下标
    private double max_density = -Double.MAX_VALUE;// 最大的局部密度

    @Override
    public void setup(Context cxt) throws IOException {
        densityPath = new Path(Url.OUTPUT_LOCAL_DENSITY_PATH);//获取局部密度文件的存储目录
        Configuration conf = HadoopConfigurationUtil.getConfig();
        FileSystem hdfs = FileSystem.get(conf);

        FileStatus[] fss = densityPath.getFileSystem(conf).listStatus(densityPath);
        for(FileStatus f:fss){//遍历存储局部密度文件目录下的文件
            if(!f.toString().contains("part")){
                continue; // 排除其他文件，只处理文件名包含“part”的文件
            }
            try {
                // 局部密度文件：  id  密度值
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(f.getPath())));
                String line;
                line = br.readLine();
                while (line != null) { // 遍历当前文件
                    String[] temp = line.split("\t");
                    int id = Integer.valueOf(temp[0]);
                    double density = Double.valueOf(temp[1]);
                    densityMap.put(id, density);
                    if(density > max_density){// 寻找最大局部密度的id
                        max_density = density;//保存当前最大局部密度
                        max_density_vector_id = id;//保存当前最大局部密度的id
                    }
                    line = br.readLine();
                }
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
                max_density_vector_id = -1;
            }
        }

        // 把max_density_vector_id写入文件
        Path path = new Path(Url.DELTADISTANCEBIN);
        if(hdfs.exists(path)){
            hdfs.delete(path, false);
        }
        FSDataOutputStream out = hdfs.create(path);
        try{
            out.writeInt(max_density_vector_id);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            out.close();
        }

    }

    // 输入    距离   id_i,id_j
    // 输出    id     距离, 密度
    @Override
    public void map(LongWritable key, Text value, Context cxt) throws IOException,InterruptedException{
        String[] temp = StringUtil.getFromDoubleAndIntPairWritable(value.toString());
        int[] userIdPair = StringUtil.getFromUserIdPair(temp[1]);
        int vectorI = userIdPair[0];
        int vectorJ = userIdPair[1];
        if(vectorI == max_density_vector_id || vectorJ == max_density_vector_id){// 最大局部密度，需寻找离该点最大的距离
            // 如果是局部密度最大点，应该直接输出即可
            vector_i.set(max_density_vector_id);
            density_distance.setFirst(max_density); // 密度
            density_distance.setSecond(Double.valueOf(temp[0])); // 距离
            cxt.write(vector_i, density_distance);
            return ;

        }
        // 不是局部密度最大点，则找最小的即可
        double densityI = 0;
        double densityJ = 0;
        if(!densityMap.containsKey(vectorI)||!densityMap.containsKey(vectorJ)){//  两个任一个不存在，则不输出任何
            return ;
        }

        densityI = densityMap.get(vectorI);
        densityJ = densityMap.get(vectorJ);

        if(densityI > densityJ){// 输出   <I,大于I 的density的distance>
            vector_i.set(vectorI);
            density_distance.setFirst(densityI);
            density_distance.setSecond(Double.valueOf(temp[0]));// second 是distance
            cxt.write(vector_i, density_distance);
        }
        if(densityI < densityJ){// 输出   <J,大于J 的density的distance>
            vector_i.set(vectorJ);
            density_distance.setFirst(densityJ);
            density_distance.setSecond(Double.valueOf(temp[0]));
            cxt.write(vector_i, density_distance);
        }
    }

}
