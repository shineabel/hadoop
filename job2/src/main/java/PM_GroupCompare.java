
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PM_GroupCompare extends WritableComparator {
    protected  PM_GroupCompare(){
        super(DataBean.class,true);
    }
    @Override
    public int compare(WritableComparable a,WritableComparable b) {

        DataBean db1 = (DataBean) a ;
        DataBean db2 = (DataBean) b ;

       
        return db1.getCityName().compareTo(db2.getCityName());
    }
}

