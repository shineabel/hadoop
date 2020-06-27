

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PM_reduce2 extends Reducer<DataBean2, DataBean2, DataBean2, NullWritable> {

    @Override
    protected void reduce(DataBean2 key, Iterable<DataBean2> values, Context context) throws IOException, InterruptedException {
    	DataBean2 dataBean = new DataBean2();
        long acount = 0;
        long bcount = 0;
        long ccount = 0;
        long dcount = 0;
        long ecount = 0;
        dataBean.setCityName(key.getCityName());
        for (DataBean2 value : values) {
            if(value.getLevelADayCount().intValue() == 1) {
            	acount ++;
            }
            if(value.getLevelBDayCount().intValue() == 1) {
            	bcount ++;
            }
            if(value.getLevelCDayCount().intValue() == 1) {
            	ccount ++;
            }
            if(value.getLevelDDayCount().intValue() == 1) {
            	dcount ++;
            }
            if(value.getLevelEDayCount().intValue() == 1) {
            	ecount ++;
            }
        }
        dataBean.setLevelADayCount(acount);
        dataBean.setLevelBDayCount(bcount);
        dataBean.setLevelCDayCount(ccount);
        dataBean.setLevelDDayCount(dcount);
        dataBean.setLevelEDayCount(ecount);
        
        context.write(dataBean,NullWritable.get());
    }
}

