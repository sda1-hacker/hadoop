package com.hive.demo.UDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

// @UDFType(deterministic = false) // 不确定的函数, 输入的参数相同，或者不输入参数，得到的结果不同 -- 自动递增的实现就是一个不确定的函数，-- 不输入参数得到的序列不相同
// @UDFType(deterministic = true)  // 确定函数, 输入的参数相同得到的结果也相同
// 自定义UDF
public class MyUDF extends UDF {

    // 方法名必须是 evaluate  -- 这个方法的返回值不能是void
    public int evaluate(String val1, String val2) {
        return val1.length() + val2.length();
    }
}
