

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class DataBean1 implements WritableComparable<DataBean1> {
    private String cityName;
    private String day;
    private double aqi = 0d;
   
    
    
	public DataBean1(String cityName, String day, double aqi) {
		super();
		this.cityName = cityName;
		this.day = day;
		this.aqi = aqi;
	}





	public DataBean1() {
		super();
	}
	
	



	public String getDay() {
		return day;
	}





	public void setDay(String day) {
		this.day = day;
	}





	public double getAqi() {
		return aqi;
	}





	public void setAqi(double aqi) {
		this.aqi = aqi;
	}





	public String getCityName() {
		return cityName;
	}
	public void setCityName(String cityName) {
		this.cityName = cityName;
	}
	
	
	
    public int compareTo(DataBean1 o) {

        int result = o.getCityName().compareTo(this.getCityName());
        if(result == 0) {
        	result = o.getDay().compareTo(this.getDay());
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(cityName);
        out.writeUTF(day);
        out.writeDouble(aqi);
        
    }

    public void readFields(DataInput in) throws IOException {

        cityName = in.readUTF();
        day = in.readUTF();
        aqi = in.readDouble();

    }

    @Override
    public String toString() {
        return  cityName +","+day +","+aqi ;
    }
}

