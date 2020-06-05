package aiproject.utils;

import org.apache.hadoop.conf.Configuration;

public class HadoopConfigurationUtil {

    public static String HADOOP_IP = "192.168.31.112";
    public static String namenode_PORT = "9000";
    public static String resourcemanager_PORT = "8032";
    public static String resource_PORT = "8030";


    public static Configuration getConfig(){
        Configuration conf = new Configuration();
        // get configuration from db or file
        conf.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
        conf.set("fs.defaultFS", "hdfs://" + HADOOP_IP + ":" + namenode_PORT);// 指定namenode
        conf.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
        conf.set("yarn.resourcemanager.address", HADOOP_IP + ":" + resourcemanager_PORT); // 指定resourcemanager
        conf.set("yarn.resourcemanager.scheduler.address", HADOOP_IP + ":" + resource_PORT);// 指定资源分配器
        conf.set("mapreduce.jobhistory.address", "0.0.0.0:10020");  //历史服务

        return conf;
    }

}
