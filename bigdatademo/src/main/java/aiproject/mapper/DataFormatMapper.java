package aiproject.mapper;

import aiproject.utils.UserCheckUtil;
import aiproject.writable.DoubleArrIntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// 格式化文件数据mapper            reputation   upVotes   downVotes    views
// 输入：去重后数据     行号,<type,用户向量>
// 输出：id（行号-int）,<用户向量>
public class DataFormatMapper extends Mapper<LongWritable, Text, IntWritable, DoubleArrIntWritable> {

    private IntWritable res_key = new IntWritable();
    private DoubleArrIntWritable res_val = new DoubleArrIntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long temp = key.get();
        res_key.set((int)temp);
        String[] temp_str = value.toString().split("\t");
        if(UserCheckUtil.checkUser(temp_str)){
            double[] temp_double_arr = {Double.valueOf(temp_str[0]), Double.valueOf(temp_str[1]),
                    Double.valueOf(temp_str[2]), Double.valueOf(temp_str[3])};
            res_val.setValue(temp_double_arr);
            context.write(res_key, res_val);
        }
        else{
            return;
        }
    }
}
