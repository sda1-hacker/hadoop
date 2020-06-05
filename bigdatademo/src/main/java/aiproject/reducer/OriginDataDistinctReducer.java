package aiproject.reducer;

import aiproject.entity.User;
import aiproject.utils.UserCheckUtil;
import aiproject.utils.XMLUtils;
import aiproject.writable.DoubleArrIntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 原始数据去重
 * email相同的认为是相同的用户， 只保留Reputation最大的数据
 * 输入：  key: emailHash --> Text ，   value: 行数据 --> Text
 * 输出:   key: <type,用户的有效向量 >      value: null
 * 用行号表示用户id
 */
public class OriginDataDistinctReducer extends Reducer<Text, Text, Text, NullWritable> {

    private Text res_key = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> itList = new ArrayList<Text>();
        for(Text temp : values){
            itList.add(temp); // 将迭代器存到List中便于执行删除操作
        }


        if( 1 == itList.size()){ // 只有一条数据
            Text temp = itList.get(0);
            User user = new User(temp);
            res_key.set(user.toString());
            context.write(res_key, NullWritable.get());
            return ;
        }

        // 去除重复记录  --  取Reputation最大的
        String strValue = null;    //Reputation对应的值
        int intValue = Integer.MAX_VALUE;
        int index = -1;
        int tmpValue = 0;
        for(int i = 0; i<itList.size(); i++){
            strValue = XMLUtils.getAttrValInLine(itList.get(i).toString(), "Reputation");
            try{
                tmpValue = Integer.parseInt(strValue);
            }catch(Exception e){
                tmpValue = intValue;
            }
            if(tmpValue < intValue){
                index = i;
                intValue = tmpValue;
            }
        }
        if(index != -1){
            Text temp = itList.get(index);
            User user = new User(temp);
            if(UserCheckUtil.checkUser(user) == true){
                res_key.set(user.toString());
                context.write(res_key, NullWritable.get());
            }

        }else{
            Text temp = itList.get(0);
            User user = new User(temp);
            if(UserCheckUtil.checkUser(user) == true){
                res_key.set(user.toString());
                context.write(res_key, NullWritable.get());
            }
        }
    }
}
