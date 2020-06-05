package aiproject.utils;

import com.sun.tools.jdi.ObjectReferenceImpl;

// url
public class Url {

    // 原始数据
    public static String INPUT_XML_PATH = "/aiproject/xml/input";

    // 去重后的数据
    public static String OUTPUT_XML_PATH1 = "/aiproject/distinct/output1";

    // 去重后的数据  --  id,<用户向量>
    public static String OUTPUT_XML_PATH = "/aiproject/distinct/output";

    // 用户距离输出目录
    public static String OUTPUT_DIS_PATH = "/aiproject/dis/output";

    // 局部密度输出目录
    public static String OUTPUT_LOCAL_DENSITY_PATH = "/aiproject/local_density/output";

    // 局部密度最大向量id存储路径
    public static String DELTADISTANCEBIN = "/aiproject/mindistance.bin/output";

    // 最小距离输出路径
    public static String DELTADISTANCEOUTPUT = "/aiproject/mindistance/output";

    // 最佳dc
    public static String BEST_DC = "/aiproject/bestdc/output";

    // 排序后结果输出路径
    public static String SORTOUTPUT = "/aiproject/sort/output";

    // 聚类中心id输出的路径
    public static String CENTER_OUTPUT = "/aiproject/center/output";

    // 已分类
    public static String CLUSTERED = "/aiproject/clustered";

    // 未分类
    public static String UNCLUSTERED = "/aiproject/unclustered";

}
