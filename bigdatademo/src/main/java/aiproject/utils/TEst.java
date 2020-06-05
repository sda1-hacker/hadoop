package aiproject.utils;

public class TEst {
    public static void main(String[] args) {

//        	1	355.0,29.0,1.0,25.0
//        	2	380.0,2.0,1.0,23.0
//        	3	166.0,10.0,1.0,11.0
//        	4	257.0,13.0,6.0,26.0


        double[] d1 = {355.0,29.0,1.0,25.0};

        double[] d2 = {380.0,2.0,1.0,23.0};

        double[] d3 = {166.0,10.0,1.0,11.0};

        double[] d4 = {257.0,13.0,6.0,26.0};

        // 168615	2	[492.0,13.0,3.0,8.0]

        // 168789	4	[265.0,14.0,1.0,16.0]

        // 161019	3	[111.0,5.0,1.0,16.0]

        // 162014	1	[314.0,53.0,3.0,16.0]

        double[] dd = {314.0,53.0,3.0,16.0};

        DistanceUtil.getDistance(d1, dd);

        System.out.println(DistanceUtil.getDistance(d1, dd));
        System.out.println(DistanceUtil.getDistance(d2, dd));
        System.out.println(DistanceUtil.getDistance(d3, dd));
        System.out.println(DistanceUtil.getDistance(d4, dd));
    }
}
