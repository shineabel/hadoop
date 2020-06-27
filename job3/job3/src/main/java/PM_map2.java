
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PM_map2 extends Mapper<LongWritable, Text, DataBean2, DataBean2> {
	

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//	站号,经度,纬度,PM25,PM10,NO2,SO2,O3-1,O3-8h,CO,AQI,等级,年，月，日，小时，城市。  ","分割。
//	99000,115.49,38.88,43,68,21,20,104,104,0.6,60,2,2018,8,1,0,北京
		String line = value.toString();
		String[] spilt = line.split(",");
		String cityName = spilt[0];
		DataBean2 dataBean = new DataBean2();
		BigDecimal pmValue = new BigDecimal(spilt[2]);

			dataBean.setCityName(cityName);
			// 0~50、51~100、101~150、151~200、201~300
			if (pmValue.compareTo(BigDecimal.ZERO) >= 0 && pmValue.compareTo(new BigDecimal(50)) <= 0) {
				dataBean.setLevelADayCount(1l);
			}
			if (pmValue.compareTo(new BigDecimal(51)) >= 0 && pmValue.compareTo(new BigDecimal(100)) <= 0) {
				dataBean.setLevelBDayCount(1l);
			}
			if (pmValue.compareTo(new BigDecimal(101)) >= 0 && pmValue.compareTo(new BigDecimal(150)) <= 0) {
				dataBean.setLevelCDayCount(1l);
			}
			if (pmValue.compareTo(new BigDecimal(151)) >= 0 && pmValue.compareTo(new BigDecimal(200)) <= 0) {
				dataBean.setLevelDDayCount(1l);
			}
			if (pmValue.compareTo(new BigDecimal(201)) >= 0 && pmValue.compareTo(new BigDecimal(300)) <= 0) {
				dataBean.setLevelEDayCount(1l);
			}
			context.write(dataBean, dataBean);
	}
}
