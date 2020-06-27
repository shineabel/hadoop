

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataBean implements WritableComparable<DataBean> {
    private String cityName;
    private double pmValue;

    public DataBean() {
        super();
    }
    public DataBean(String cityName, int pmValue) {
        super();
        this.cityName = cityName;
        this.pmValue = pmValue;
    }


    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public double getPmValue() {
        return pmValue;
    }

    public void setPmValue(double pmValue) {
        this.pmValue = pmValue;
    }


    public int compareTo(DataBean o) {

        return o.getCityName().compareTo(this.getCityName());
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(cityName);
        out.writeDouble(pmValue);
    }

    public void readFields(DataInput in) throws IOException {

        cityName = in.readUTF();
        pmValue = in.readDouble();

    }

    @Override
    public String toString() {
        String pmValueStr = String.format("%.2f",pmValue);
        return  cityName +"\t"+pmValueStr ;
    }
}

