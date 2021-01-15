package com.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyKafkaInterceptor implements ProducerInterceptor<String, String> {

    // 在序列化之前就调用 -- 每次发消息之前都要调用
    // 接受producerRecord进行处理，处理完毕后再发送给给后面的阶段继续处理
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 可以在这里对消息进行处理。
        return null;
    }

    // 在服务器给出ack应答的时候调用。
    // recordMetadata是元数据。
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    // producer调用close 或者 producer对象被回收的时候调用这个方法。
    @Override
    public void close() {

    }

    // 初始化拦截器的时候调用这个方法。 -- 最早执行
    @Override
    public void configure(Map<String, ?> map) {

    }
}
