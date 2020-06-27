
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PM_GroupCompare2 extends WritableComparator {
    protected  PM_GroupCompare2(){
        super(DataBean2.class,true);
    }
    @Override
    public int compare(WritableComparable a,WritableComparable b) {

        DataBean2 db1 = (DataBean2) a ;
        DataBean2 db2 = (DataBean2) b ;

       
        return db1.getCityName().compareTo(db2.getCityName());
    }
}

