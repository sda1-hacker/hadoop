package com.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ZkClientTest {


    // 单机版ZK
    private final String ZK_SERVER = "192.168.157.129:2181";

    // 集群版ZK
    private final String ZK_CLUSTER = "192.168.157.129:2181,192.168.157.130:2181,192.168.157.131:2181";

    // client会自动向zk后端发送心跳
    private CuratorFramework client;

    // 获取到zk客户端
    @Before // 运行在@Test之前
    public void initZkClient() {
        // 1. 创建zk连接
        // 有断开重连的机制。 如果断开了连接，会尝试多次重连， 重试10次， 两次重试之间的间隔为3S
        RetryNTimes retry = new RetryNTimes(10, 3000);

        // 2. 基于这个连接创建一个client
        // 集群地址信息, 连接
        client = CuratorFrameworkFactory.newClient(ZK_SERVER, retry);

        // 3. 启动客户端  -- 客户端需要向zk发送心跳，因此必须要保持客户端属于启动的状态
        client.start();
    }


    // zk基础操作
    @Test
    public void Zktest() throws Exception {

        // 判断节点是否存在 -- 如果不存在返回null, 如果存在返回节点的描述信息
        Stat status = client.checkExists().forPath("/sda1_hacker");


        // 节点存在 -- 删除对应的节点
        if (status != null) {
            client.delete().forPath("/sda1_hacker");
            System.out.println("节点存在, 已删除");
        }

        System.out.println("---------");


        // 创建一个临时节点, 返回节点名字 -- 默认存活时间为30S
        String zNodeName = client.create().withMode(CreateMode.EPHEMERAL).forPath("/sda1_hacker", "今天周五".getBytes());
        System.out.println(zNodeName);

        System.out.println("---------");


        // 获取节点对应的数据
        byte[] data = client.getData().forPath("/sda1_hacker");
        String resData = new String(data);
        System.out.println(resData);

        System.out.println("---------");


        // 修改节点数据 -- 修改成功返回节点描述信息, 否则返回null
        Stat updateStatus = client.setData().forPath("/sda1_hacker", "去吃火锅".getBytes());

        if (updateStatus != null) {
            System.out.println("修改成功");
            byte[] data1 = client.getData().forPath("/sda1_hacker");
            String resData1 = new String(data1);
            System.out.println(resData1);
        }

        System.out.println("---------");


        // 获取某个节点下的子节点
        List<String> zNodeNameList = client.getChildren().forPath("/sda1_hacker");
        zNodeNameList.forEach(name -> {
            System.out.println(name);
        });

        System.out.println("---------");

    }


    // zk监控 -- 单节点
    @Test
    public void zkWatchTestSimple() throws Exception {

        // 1. 创建一个线程池, 用来执行监听事件触发的函数
        ExecutorService pool = Executors.newFixedThreadPool(5);

        // 2. 基于client创建出一个监听器
        // client, 监听的路径
        NodeCache nodeCache = new NodeCache(client, "/sda1_hacker");

        // 3. 启动监听器
        nodeCache.start();

        // 4. 事件出发后的回调函数 -- 从线程池中拿出一个线程来执行
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            // 节点的值发生改变，或者节点被删除，这个函数才会执行  -- 创建, 修改, 删除
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("--- 节点发生改变 ---");
                byte[] data = nodeCache.getCurrentData().getData(); // 被改变节点的值
                String resData = new String(data);
                System.out.println("---" + resData);
            }
        }, pool);

        // 5. 一直监听
        Thread.sleep(Long.MAX_VALUE);

    }


    // zk监控 -- 子节点 -- 监控某一个节点下面的所有节点
    @Test
    public void zkWatchTestChildNode() throws Exception {

        // 1. 创建线程池，用来执监听事件出发的函数
        ExecutorService pool = Executors.newFixedThreadPool(5);

        // 2. 基于client创建一个监听器
        // 客户端, 监听的节点, 子节点的数据是否缓存到本地
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/sda1_hacker", true);

        // 3. 启动监听器
        pathChildrenCache.start();

        // 4. 事件触发之后的回调函数
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            // 子节点创建， 或者删除

            /**
             * @param client    当前zk客户端
             * @param event     事件对象 -- 添加，删除，修改 -- 就是当前被操作的子节点
             * @throws Exception
             */
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("/sda1_hacker下面的子节点上线或者下线");

                PathChildrenCacheEvent.Type type = event.getType();  // 当前操作节点的类型， 添加， 删除，修改
                ChildData data = event.getData();// 节点对象
                data.getPath(); // 路径
                data.getData(); // 值

            }
        }, pool);

        Thread.sleep(Long.MAX_VALUE);

    }

}
