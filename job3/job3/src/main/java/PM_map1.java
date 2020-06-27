
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PM_map1 extends Mapper<LongWritable, Text, DataBean1, DataBean1> {
	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//	站号,经度,纬度,PM25,PM10,NO2,SO2,O3-1,O3-8h,CO,AQI,等级,年，月，日，小时，城市。  ","分割。
//	99000,115.49,38.88,43,68,21,20,104,104,0.6,60,2,2018,8,1,0,北京
		String line = value.toString();
		String[] spilt = line.split(",");
		String cityName = spilt[16];
		String y = spilt[12];
		String m = spilt[13];
		String d = spilt[14];
		DataBean1 dataBean = new DataBean1();
		double  aqiValue =  Double.parseDouble(spilt[10]);

		if (y.equals("2019") && m.equals("2")
				&& (cityName.equals("北京") || cityName.equals("上海") || cityName.equals("成都"))) {
			dataBean.setCityName(cityName);
			dataBean.setAqi(aqiValue);
			dataBean.setDay(y + "-" + m  + "-" + d);
			context.write(dataBean, dataBean);
		}
	}
}
