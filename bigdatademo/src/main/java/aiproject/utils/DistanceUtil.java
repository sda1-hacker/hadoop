package aiproject.utils;

import java.util.Arrays;

public class DistanceUtil {

    public static long INPUT_RECORDS = 0L; // 距离文件的总记录数

    // 向量距离
    public static double getDistance(double[] v1, double[] v2) {
        double dis = 0.0;
        for (int i = 0; i < v1.length; i++) {
            dis += (v1[i] - v2[i]) * (v1[i] - v2[i]);
        }
        return Math.sqrt(dis);
    }

    /**
     * 获取给定数组两两之间的距离(欧式距离:HUtils.getDistance() )，每个行为一个向量
     * 并把这些距离按照大小从小到大排序
     * @param vectors
     * @return
     */
    public static double[] getDistances(double[][] vectors) {
        double[] distances= new double[vectors.length*(vectors.length-1)/2];
        int k=0;
        for(int i=0;i<vectors.length;i++){
            for(int j=i+1;j<vectors.length;j++){
                distances[k++] = DistanceUtil.getDistance(vectors[i], vectors[j]);
            }
        }
        // sort
        Arrays.sort(distances);
        return distances;
    }

}
