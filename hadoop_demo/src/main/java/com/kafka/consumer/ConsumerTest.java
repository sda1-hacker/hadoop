package com.kafka.consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Map;

public class ConsumerTest {

    public static void main(String[] args) {
        Map<String, Object> config = Maps.newHashMap();

        // kafka地址
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.157.129:9092");
        // key反序列化
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // value反序列化
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 消费者需要配置group id -- 如果没有不运行
        // kafka中消费者必须属于一个组
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        // 新的consumer从头开始消费
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 手动提交offset
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // consumer对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);

        // 订阅tpoic
        consumer.subscribe(Arrays.asList("mymsg"));

        while (true) {
            // 到topic中拿数， 队列中没有数据的时候会延迟一秒再去拿数据
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord record : records) {
                System.out.println(record.value().toString());
            }
            consumer.commitAsync(); // 异步提交
            consumer.commitSync();  // 同步提交
        }

    }
}
