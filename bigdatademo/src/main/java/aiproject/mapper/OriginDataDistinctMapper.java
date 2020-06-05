package aiproject.mapper;

import aiproject.utils.XMLUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 原始数据去重
 * email相同的认为是相同的用户
 * 输出：  key: emailHash --> Text，   value: 行数据 --> Text
 */
public class OriginDataDistinctMapper extends  Mapper<LongWritable, Text, Text, Text>{

    private Text emailHashKey = new Text();
    private String attr ="EmailHash";   // 值提取的规则

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String val = value.toString();

        if(!val.trim().startsWith("<row")){
            return ;
        }

        try{
            String emailHash = XMLUtils.getAttrValInLine(val, attr); // emailHash对应的值
            emailHashKey.set(emailHash);
            context.write(emailHashKey, value);
        }
        catch (Exception e){
            System.out.println("email提取失败");
            return ;
        }
    }
}
