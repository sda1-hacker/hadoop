package com.flume.demo;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    // 单个event -- 业务逻辑写这里
    @Override
    public Event intercept(Event event) {

        // body
        String body = new String(event.getBody());

        // header
        Map<String, String> header = event.getHeaders();

        if (NumberUtils.isDigits(body)) {
            header.put("status", "number");
        } else {
            header.put("status", "letter");
        }

        return event;
    }

    // event列表 -- 业务逻辑写这里
    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> resList = new ArrayList<>();
        for (Event event : list) {
            resList.add(intercept(event));
        }
        return resList;
    }

    @Override
    public void close() {

    }

    // 固定写法 -- 让flume底层获取自定义拦截器对象
    public static class MyBuilder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new MyInterceptor(); // 自定义拦截器
        }

        @Override
        public void configure(Context context) {

        }
    }
}
