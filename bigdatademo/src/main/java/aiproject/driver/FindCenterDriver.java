package aiproject.driver;


import aiproject.utils.CentIdUtil;
import aiproject.utils.SeqFileUtil;
import aiproject.utils.Url;

import java.util.List;
import java.util.Map;

// 第八步，寻找聚类中心
public class FindCenterDriver {

    // 1. 读取SortJob的输出，获取前面k条记录中的大于局部密度和最小距离阈值的id；
    // 2. 根据id，找到每个id对应的记录；
    // 3. 把记录转为double[] ；
    // 4. 把向量写入hdfs
    public static void main(String[] args) {

        Map<String, String> firstK =null;
        List<Integer> ids = null;

        try{
            firstK = SeqFileUtil.readSeq(Url.SORTOUTPUT+"/part-r-00000", 100);// 这里默认读取前100条记录
            // 局部密度阈值 13, // 最小距离阈值25    由决策图得出
            ids = CentIdUtil.getCentIds(firstK, args[0], args[1]);

            // ids = CentIdUtil.getCentIds(firstK, "125", "0.0");

            System.out.println("聚类中心个数: " + ids.size());

            CentIdUtil.writecenter2hdfs(ids, Url.CENTER_OUTPUT);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
