
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PM_GroupCompare1 extends WritableComparator {
    protected  PM_GroupCompare1(){
        super(DataBean1.class,true);
    }
    @Override
    public int compare(WritableComparable a,WritableComparable b) {

        DataBean1 db1 = (DataBean1) a ;
        DataBean1 db2 = (DataBean1) b ;

       
        int result = db1.getCityName().compareTo(db2.getCityName());
        if(result == 0) {
        	result = db1.getDay().compareTo(db2.getDay());
        }
        return result;
    }
}

