package com.kafka.producer;

import com.google.common.collect.Maps;
import com.kafka.partition.MyPartition;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class ProducerTest {

    public static void main(String[] args) {
        Map<String, Object> config = Maps.newHashMap();

        // kafka地址
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.157.129:9092");
        // key序列化
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // value序列化
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 使用自定义分区
        // config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartition.class);

        // acks = -1
        config.put(ProducerConfig.ACKS_CONFIG, -1);
        // 开启幂等性
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 使用自定义拦截器，写全类名，如果有多个，用逗号隔开
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.kafka.interceptor.MyKafkaInterceptor");

        // 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer(config);

        // 向top中发送消息
        // topic提前通过命令创建好
        for (int i = 0; i < 10; i++ ) {
            producer.send(new ProducerRecord<>("mymsg", "hello kafka" + i));
        }

        // 发送后会异步调用一个函数。
        for (int i = 0; i < 10; i++ ) {
            producer.send(new ProducerRecord<>("mymsg", "hello kafka" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("消息发送成功");
                        System.out.println("offset: " + recordMetadata.offset()); // 消息的位置
                        System.out.println("topic: " + recordMetadata.topic());
                        System.out.println("partition: " + recordMetadata.partition());

                    }
                }
            });
        }

        producer.close();
    }
}
