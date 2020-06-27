

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.IOException;

public class PM_reduce extends Reducer<DataBean, DoubleWritable, DataBean, NullWritable> {
    private long count;
    private double sum;

    @Override
    protected void reduce(DataBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        sum = 0;
        count = 0;
        DoubleWritable v = new DoubleWritable();
        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }
        double avr =  sum/count;
        v.set(avr);
        key.setPmValue(avr);
        context.write(key,NullWritable.get());
    }
}

