
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PM_job2  {
    
    public static void main(String[] args) throws Exception {
    	Configuration configuration = new Configuration();
    	 //1.获取job
        Job job =  Job.getInstance(configuration);
        //2.设置jar包路径
        job.setJarByClass(PM_job2.class);
        job.setMapperClass(PM_map2.class);
        job.setReducerClass(PM_reduce2.class);
        //设置最后 kv类型
        job.setOutputKeyClass(DataBean2.class);
        job.setOutputValueClass(NullWritable.class);
        //设置map输出类型
        job.setMapOutputKeyClass(DataBean2.class);
        job.setMapOutputValueClass(DataBean2.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setGroupingComparatorClass(PM_GroupCompare2.class);
        job.waitForCompletion(true);
    }

}

