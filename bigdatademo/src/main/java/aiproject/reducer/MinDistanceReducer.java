package aiproject.reducer;

import aiproject.utils.HadoopConfigurationUtil;
import aiproject.utils.Url;
import aiproject.writable.DoublePairWritable;
import aiproject.writable.IntDoublePairWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// 输出   密度距离的乘积     id,距离,密度
public class MinDistanceReducer extends Reducer<IntWritable, DoublePairWritable, DoubleWritable, IntDoublePairWritable> {

    private IntDoublePairWritable i_density_distance = new IntDoublePairWritable();
    private DoubleWritable mul = new DoubleWritable();

    private int max_density_vector_id = -1;

    @Override
    public void setup(Context cxt) throws IOException{
        Path path = new Path(Url.DELTADISTANCEBIN);
        FileSystem hdfs = FileSystem.get(HadoopConfigurationUtil.getConfig());
        FSDataInputStream in = hdfs.open(path);
        // 读取最大局部密度
        try{
            max_density_vector_id = in.readInt();
        }catch(IOException e){
            e.printStackTrace();
        }finally{
            in.close();
        }
    }

    @Override
    public void reduce(IntWritable key,Iterable<DoublePairWritable> values,Context cxt) throws IOException,InterruptedException{
        // 如果是最大局部密度，则寻找最大距离，否则寻找最小距离
        double minDistance = key.get()!=max_density_vector_id?Double.MAX_VALUE:-Double.MAX_VALUE;
        double density=0.0;
        for(DoublePairWritable s:values){

            if(key.get() != max_density_vector_id && s.getSecond() < minDistance){// 寻找距离最小的
                minDistance = s.getSecond();
                density=s.getFirst();
            }
            if(key.get() == max_density_vector_id && s.getSecond() > minDistance){// 寻找距离最大的
                minDistance = s.getSecond();
                density=s.getFirst();
            }
        }

        i_density_distance.setFirst(density);       // 密度
        i_density_distance.setSecond(minDistance); //  距离
        i_density_distance.setThird(key.get());  // id

        mul.set(density*minDistance);
        cxt.write(mul, i_density_distance);

    }
}
