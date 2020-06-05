package aiproject.utils;

// 解析从文件中读取的字符串
public class StringUtil {

    // 参数格式：id  type    [a,b,c,d]
    public static String[] getFromDoubleArrIntWritable(String text){
        String[] res_str = text.split("\t");
        res_str[2] = res_str[2].replace("[","");
        res_str[2] = res_str[2].replace("]","");

        return res_str;
    }

    // a,b,c,d   转为 double 数组
    public static double[] getFromVector(String vector){
        String[] temp = vector.split(",");
        double[] vec = {Double.valueOf(temp[0]),Double.valueOf(temp[1]),
                Double.valueOf(temp[2]),Double.valueOf(temp[3])};
        return vec;
    }


    // 距离  用户i,用户j
    public static String[] getFromDoubleAndIntPairWritable(String text){
        String[] res_str = text.split("\t");
        return res_str;
    }

    // 用户1,用户j
    public static int[] getFromUserIdPair(String userIdPair){
        String[] temp = userIdPair.split(",");
        int[] res_int = {Integer.valueOf(temp[0]), Integer.valueOf(temp[1])};
        return res_int;
    }

    public static void main(String[] args) {
//        String[] res = StringUtil.getFromDoubleArrIntWritable("id\ttype\t[1.0,20.0,3.0,4.0]");
//        double[] vec = StringUtil.getFromVector(res[2]);
//        for(double c : vec){
//            System.out.println(c);
//        }

        String[] res = StringUtil.getFromDoubleAndIntPairWritable("2.00\t566,789");
        int[] res_int = StringUtil.getFromUserIdPair(res[1]);
        for(int c : res_int){
            System.out.println(c);
        }
    }
}
