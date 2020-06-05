/**
 *
 */
package aiproject.utils;

import java.awt.Font;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

import aiproject.entity.IDistanceDensityMul;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

// 第七步，画决策图
public class DrawPic {

//	此图用于寻找聚类中心，看图找到聚类中心的范围 x轴大于多少，y轴大于多少，在寻找聚类中心
//	注意：决策图展示的不包含局部密度最大的点，所以在执行分类时需要把决策图中的聚类中心个数加1


	public static void main(String[] args) throws IOException {
		String path = Url.SORTOUTPUT + "/part-r-00000";
		String file = "C:\\Users\\Administrator\\Desktop\\decision_chart.png";
		drawPic(path, file);
	}


	public static void drawPic(String url,String file) throws FileNotFoundException, IOException {
		XYSeries xyseries = getXYseries(url); // 数据源
		XYSeriesCollection xyseriescollection = new XYSeriesCollection(); // 用XYSeriesCollection添加入XYSeries
		xyseriescollection.addSeries(xyseries);

		// 主题
		StandardChartTheme standardChartTheme = new StandardChartTheme("CN");
		// 标题
		standardChartTheme.setExtraLargeFont(new Font("宋书", Font.BOLD, 20));
		// 图例
		standardChartTheme.setRegularFont(new Font("宋书", Font.PLAIN, 15));
		// 坐标轴
		standardChartTheme.setLargeFont(new Font("宋书", Font.PLAIN, 15));
		// 应用样式
		ChartFactory.setChartTheme(standardChartTheme);
		JFreeChart chart = ChartFactory.createScatterPlot("PIC", "局部密度",
				"相对距离", xyseriescollection, PlotOrientation.VERTICAL, true,
				false, false);//产生绘制结果
		try {
			ChartUtilities.saveChartAsPNG(new File(file), chart,
					470, 470);//保存图片
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * 每个数据文件提取最多500个数据点，
	 * 前面500条记录包含的局部密度和最小距离的乘积的最大的500个，后面的点更不可能成为聚类中心点。
	 * 在提取完成之后，获得所有数据的前100个最大的数据点存储在本地文件中
	 * 供在用户看决策图后选择类别后写入中心点
	 * 每个数据点文件需要先进行排序（从大到小，所以在MR任务中增加了一个）
	 * @param url
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static XYSeries getXYseries(String url) throws FileNotFoundException, IOException{
		XYSeries xyseries = new XYSeries("");

		List<IDistanceDensityMul> list = getIDistanceDensityMulList(url);

		for(IDistanceDensityMul l:list){
			xyseries.add(l.getDensity(), l.getDistance());
		}

		return xyseries;
	}

	private static List<IDistanceDensityMul> getIDistanceDensityMulList(String url) throws FileNotFoundException, IOException{
		Configuration conf = HadoopConfigurationUtil.getConfig();
		FileSystem hdfs = FileSystem.get(conf);
		// 多个文件整合，需排序
		List<IDistanceDensityMul> allList= new ArrayList<IDistanceDensityMul>();
		// 单个文件
		List<IDistanceDensityMul> fileList= new ArrayList<IDistanceDensityMul>();

		FileStatus[] fss= FileSystem.get(conf)
				.listStatus(new Path(url));
		for(FileStatus f:fss){
			if(!f.toString().contains("part")){
				continue; // 排除其他文件
			}
			try {
				// 密度距离的乘积   id,距离,密度
				BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(url))));
				String line;
				line = br.readLine();
				int i = 500;//默认设置为500个点
				while (line != null && i>0) { // 遍历当前文件
					i--;
					String[] temp = line.split("\t");
					String[] id_dis_des = temp[1].split(",");
					fileList.add(new IDistanceDensityMul(Double.valueOf(id_dis_des[1]),
							Double.valueOf(id_dis_des[2]),Integer.valueOf(id_dis_des[0]),
							Double.valueOf(temp[0])));// 每个文件都是从小到大排序的
					line = br.readLine();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			// 整合当前文件的前面若干条记录
			if(allList.size()<=0){// 第一次可以全部添加
				allList.addAll(fileList);
			}else{
				combineLists(allList,fileList);
			}
		}//for
		//第一个点太大了，选择去掉，否则绘制结果图比例失衡，难以寻找聚类中心。但是需要明白去掉的这个点也是一个聚类中心
		return allList.subList(1, allList.size());
	}

	/**
	 * 按照mul的值进行排序，从大到小排序
	 * @param list1
	 * @param list2
	 */
	private static void combineLists(List<IDistanceDensityMul> list1,
			List<IDistanceDensityMul> list2) {
		List<IDistanceDensityMul> allList = new ArrayList<IDistanceDensityMul>();
		int sizeOne=list1.size();
		int sizeTwo = list2.size();

		int i,j;
		for(i=0,j=0;i<sizeOne&&j<sizeTwo;){
			if(list1.get(i).greater(list2.get(j))){
				allList.add(list1.get(i++));
			}else{
				allList.add(list2.get(j++));
			}
		}
		if(i<sizeOne){// list1 has not finished
			allList.addAll(list1.subList(i, sizeOne));
		}
		if(j<sizeTwo){// list2 has not finished
			allList.addAll(list2.subList(j, sizeTwo));
		}
		// 重新赋值
		list1.clear();
		list1.addAll(allList);
		allList.clear();
	}

}
