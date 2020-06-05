package aiproject.utils;

import aiproject.entity.User;

public class UserCheckUtil {

    /**
     * 检查用户是否符合规则
     * 规则 ：reputation>15,upVotes>0,downVotes>0,views>0的用户
     * @param user
     * @return
     */
    public static boolean checkUser(User user) {

        int reputation = Integer.valueOf(user.getReputation());
        int upVotes = Integer.valueOf(user.getUpVotes());
        int downVotes = Integer.valueOf(user.getDownVotes());
        int views = Integer.valueOf(user.getViews());

        if( reputation <=15 || upVotes<=0 || downVotes<=0 || views<=0){
            return false;
        }else{
            return true;
        }
    }

    // 用户合法性检测
    public static boolean checkUser(String[] arr) {

        // reputation   upVotes   downVotes    views
        int reputation = Integer.valueOf(arr[0]);
        int upVotes = Integer.valueOf(arr[1]);
        int downVotes = Integer.valueOf(arr[2]);
        int views = Integer.valueOf(arr[3]);

        if( reputation <=15 || upVotes<=0 || downVotes<=0 || views<=0){
            return false;
        }else{
            return true;
        }
    }

    }
