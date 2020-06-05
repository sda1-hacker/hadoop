package aiproject.utils;

import aiproject.writable.DoubleArrStrWritable;
import aiproject.writable.DoublePairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.omg.PortableInterceptor.INACTIVE;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class SeqFileUtil {

    public static void readSeq(String url, String localPath) {
        Path path = new Path(url);
        Configuration conf = HadoopConfigurationUtil.getConfig();
        SequenceFile.Reader reader = null;
        FileWriter writer = null;
        BufferedWriter bw = null;
        try {
            writer = new FileWriter(localPath);
            bw = new BufferedWriter(writer);
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path),
                    SequenceFile.Reader.bufferSize(4096), SequenceFile.Reader.start(0));
            DoubleArrStrWritable dkey = (DoubleArrStrWritable) ReflectionUtils
                    .newInstance(reader.getKeyClass(), conf);
            DoublePairWritable dvalue = (DoublePairWritable) ReflectionUtils
                    .newInstance(reader.getValueClass(), conf);

            while (reader.next(dkey, dvalue)) {// 循环读取文件
                bw.write(dvalue.getFirst() + "," + dvalue.getSecond());
                bw.newLine();
            }
            System.out.println(new java.util.Date() + "ds file:" + localPath);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
                bw.close();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 读取给定序列文件的前面k条记录
     * @param input
     * @param k
     * @return
     */
    public static Map<String, String> readSeq(String input, int k) throws IOException {
        Map<String, String> map= new HashMap<String, String>();
        Path path = new Path(input);
        Configuration conf = HadoopConfigurationUtil.getConfig();
        FileSystem hdfs = FileSystem.get(conf);

        try {
            // 密度距离的乘积     id,距离,密度
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            String line;
            line = br.readLine();
            while (line != null && k>0) { // 遍历当前文件
                k--;
                // 129.49131244990917	51475,16.186414056238647,8.0
                String[] temp = line.split("\t");
                map.put(temp[1], temp[0]); //    key: id,距离,密度  value: 密度距离的乘积
                line = br.readLine();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }


    /**
     * 读取聚类中心文件内容
     * 141132	3	397.0,6.0,1.0,11.0
     * @param input
     * @param k
     * @return
     */
    public static Map<String, String> readCenterFile(String input, int k) throws IOException {
        Map<String, String> map= new HashMap<String, String>();
        Path path = new Path(input);
        Configuration conf = HadoopConfigurationUtil.getConfig();
        FileSystem hdfs = FileSystem.get(conf);

        try {
            // id   type    a,b,c,d
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
            String line;
            line = br.readLine();
            while (line != null && k>0) { // 遍历当前文件
                k--;
                // 141132	3	397.0,6.0,1.0,11.0
                String[] temp = line.split("\t");
                map.put(temp[0], temp[2]);
                line = br.readLine();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return map; //    key: id  value: a,b,c,d
    }

}
