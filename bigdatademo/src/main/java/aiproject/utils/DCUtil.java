package aiproject.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

// 寻找最佳DC
public class DCUtil {

    // 距离文件总记录数
    public static double getBestDC(long INPUT_RECORDS) throws IOException {

        double best_dc = 0.0;  // DC阈值，
        // 阀值百分比
        // 阈值百分比一般为1~2%
        double percent = 0.02;

        Path input = new Path(Url.OUTPUT_DIS_PATH + "/part-r-00000"); // 距离文件

        long iNPUT_RECORDS2 = INPUT_RECORDS; // 距离文件总数

        Configuration conf = HadoopConfigurationUtil.getConfig();

        FileSystem hdfs = FileSystem.get(conf);

        long counter = 0;
        // 因为距离计算的MapReduce任务输出文件自动按照用户间的距离从小到大排序，所以计算阀值只需截取操作即可。
        long percent_ = (long) (percent * iNPUT_RECORDS2);// 计算截取的下标值（按前2%计算）

        try{
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(input)));
            String line;
            line = br.readLine();
            while (line != null) { // 遍历当前文件
                counter++;
                // 从距离文件中提取距离
                String[] temp = StringUtil.getFromDoubleAndIntPairWritable(line);
                double dc = Double.valueOf(temp[0]);
                if (counter >= percent_) {// 遍历到了阀值下标处
                    best_dc = dc;// 赋予最佳DC阈值
                    break;
                }
                line = br.readLine();
            }
            br.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return best_dc;   // 返回最佳阈值
    }

}
