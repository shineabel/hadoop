

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PM_reduce1 extends Reducer<DataBean1, DataBean1, DataBean1, NullWritable> {

	private long count;
    private double sum;
    
    @Override
    protected void reduce(DataBean1 key, Iterable<DataBean1> values, Context context) throws IOException, InterruptedException {
    	DataBean1 dataBean = new DataBean1();
       
        dataBean.setCityName(key.getCityName());
        dataBean.setDay(key.getDay());
        
        sum = 0;
        count = 0;
        for (DataBean1 value : values) {
            sum += value.getAqi();
            count++;
        }
        double avr =  sum/count;
       dataBean.setAqi(avr);
        
        
        context.write(dataBean,NullWritable.get());
    }
}

