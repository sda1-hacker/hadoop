package aiproject.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CentIdUtil {

    // 获取聚类中心的id
    public static List<Integer> getCentIds(Map<String, String> firstK, String numReducerDensity, String numReducerDistance) {
        List<Integer> ids = new ArrayList<Integer>();

        for(String temp: firstK.keySet()){// 只查找前firstK.values()个记录

            System.out.println(temp);

            String[] id_dis_des = temp.split(",");

            if(Double.valueOf(id_dis_des[2])>Double.parseDouble(numReducerDensity)&&
                    Double.valueOf(id_dis_des[1])>Double.parseDouble(numReducerDistance)){//大于两个阀值，满足条件
                ids.add(Integer.valueOf(id_dis_des[0]));
            }
        }
        return ids;
    }

    /**
     * 每次写入聚类中心之前，需要把前一次的结果删掉，防止重复,不应该在这里删除，应该在执行分类的时候删除
     * @param output
     * @throws IOException
     */
    public static void writecenter2hdfs(List<Integer> ids, String output) throws IOException {
        // 写入到hdfs
        Configuration conf = HadoopConfigurationUtil.getConfig();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            FSDataOutputStream out = hdfs.create(new Path(output));
            int k = 1;
            for (Integer id : ids) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(Url.OUTPUT_XML_PATH + "/part-r-00000"))));
                String line;
                line = br.readLine();
                while (line != null) { // 遍历当前文件
                    String[] temp = StringUtil.getFromDoubleArrIntWritable(line);
                    int _id = Integer.valueOf(temp[0]);
                    if( id == _id){
                        out.writeBytes(temp[0] + "\t" + k + "\t" + temp[2] + "\n");   // id   type  a,b,c,d
                        k++;
                    }
                    line = br.readLine();
                }
                br.close();
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }


    /**
     *  提取Map中的center中心向量
     * @param vectorsMap
     * @return
     */
    public static double[][] getCenterVector(Map<String, String> vectorsMap) {

        double[][] centers = new double[vectorsMap.size()][];
        int i=0;
        for(String key : vectorsMap.keySet()){

            System.out.println("key:" + key + "val:" + vectorsMap.get(key));

            String[] temp = vectorsMap.get(key).split(",");
            double[] temp_d = {Double.parseDouble(temp[0]), Double.parseDouble(temp[1]),
                    Double.parseDouble(temp[2]), Double.parseDouble(temp[3])};

            centers[i++] = temp_d;
        }

        return centers;
    }
}

