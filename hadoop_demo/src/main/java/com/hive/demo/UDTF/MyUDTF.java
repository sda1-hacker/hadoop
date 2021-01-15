package com.hive.demo.UDTF;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;

// 自定义udtf -- 将 "aaa,bbb,ccc,ddd" 字符串通过指定的分隔符分割
public class MyUDTF extends GenericUDTF {

    private List<String> resList = Lists.newArrayList();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 默认输出结果的列名
        List<String> fieldNames = Lists.newArrayList();
        // 输出结果列的类型
        List<ObjectInspector> fieldType = Lists.newArrayList();

        // 默认列名
        fieldNames.add("---");
        // 这一列对应的类型
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldType);
    }

    // objects -- 参数列表
    @Override
    public void process(Object[] objects) throws HiveException {
        String arg = objects[0].toString().trim();
        String char_split = objects[1].toString().trim();

        String[] res = arg.split(char_split);

        // hive规定，使用udtf需要将结果放在集合中然后输出
        for (String str : res) {
            resList.clear();    // 清空列表
            resList.add(str);   // 将元素添加到列表
            forward(resList);   // 输出
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
