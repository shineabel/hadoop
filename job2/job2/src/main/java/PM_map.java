

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class PM_map extends Mapper<LongWritable, Text, DataBean, DoubleWritable> {
        DataBean db = new DataBean();
        DoubleWritable v = new DoubleWritable(0);

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//	站号,经度,纬度,PM25,PM10,NO2,SO2,O3-1,O3-8h,CO,AQI,等级,年，月，日，小时，城市。  ","分割。
//	99000,115.49,38.88,43,68,21,20,104,104,0.6,60,2,2018,8,1,0,北京
        String line = value.toString();
        String[] spilt = line.split(",");
        String cityName = spilt[16];
        double  pmValue =  Double.parseDouble(spilt[3]);

        db.setCityName(cityName);
        db.setPmValue(pmValue);

        v.set(pmValue);
        context.write(db,v);
        }
}

