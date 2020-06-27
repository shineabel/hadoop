

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class DataBean2 implements WritableComparable<DataBean2> {
    private String cityName;
    private Long levelADayCount = 0l;
    private Long levelBDayCount = 0l;
    private Long levelCDayCount = 0l;
    private Long levelDDayCount = 0l;
    private Long levelEDayCount = 0l;
    
    
	public DataBean2() {
		super();
	}
	
	
	public DataBean2(String cityName, Long levelADayCount, Long levelBDayCount, Long levelCDayCount, Long levelDDayCount,
			Long levelEDayCount) {
		super();
		this.cityName = cityName;
		this.levelADayCount = levelADayCount;
		this.levelBDayCount = levelBDayCount;
		this.levelCDayCount = levelCDayCount;
		this.levelDDayCount = levelDDayCount;
		this.levelEDayCount = levelEDayCount;
	}


	public String getCityName() {
		return cityName;
	}
	public void setCityName(String cityName) {
		this.cityName = cityName;
	}
	
	public Long getLevelADayCount() {
		return levelADayCount;
	}
	public void setLevelADayCount(Long levelADayCount) {
		this.levelADayCount = levelADayCount;
	}
	public Long getLevelBDayCount() {
		return levelBDayCount;
	}
	public void setLevelBDayCount(Long levelBDayCount) {
		this.levelBDayCount = levelBDayCount;
	}
	public Long getLevelCDayCount() {
		return levelCDayCount;
	}
	public void setLevelCDayCount(Long levelCDayCount) {
		this.levelCDayCount = levelCDayCount;
	}
	public Long getLevelDDayCount() {
		return levelDDayCount;
	}
	public void setLevelDDayCount(Long levelDDayCount) {
		this.levelDDayCount = levelDDayCount;
	}
	public Long getLevelEDayCount() {
		return levelEDayCount;
	}
	public void setLevelEDayCount(Long levelEDayCount) {
		this.levelEDayCount = levelEDayCount;
	}
	
    public int compareTo(DataBean2 o) {

        return o.getCityName().compareTo(this.getCityName());
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(cityName);
        out.writeLong(levelADayCount);
        out.writeLong(levelBDayCount);
        out.writeLong(levelCDayCount);
        out.writeLong(levelDDayCount);
        out.writeLong(levelEDayCount);
    }

    public void readFields(DataInput in) throws IOException {

        cityName = in.readUTF();
        levelADayCount = in.readLong();
        levelBDayCount = in.readLong();
        levelCDayCount = in.readLong();
        levelDDayCount = in.readLong();
        levelEDayCount = in.readLong();

    }

    @Override
    public String toString() {
        return  cityName +"\t,"+levelADayCount +"\t,"+levelBDayCount +"\t,"+levelCDayCount +"\t,"+levelDDayCount +"\t,"+levelEDayCount ;
    }
}

