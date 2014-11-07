package TempCalcs;

import java.io.Serializable;

public class TempData implements Serializable{
    private String year;
    private int temp;
    private String quality;
    
    public TempData(int temperature, String code, String year) {
        this.temp = temperature;
        this.quality = code;
        this.year = year;
    }
    
    public int getTemp() {
        return temp;
    }
    public void setTemp(int temp) {
        this.temp = temp;
    }
    public String getQuality() {
        return quality;
    }
    public void setQuality(String quality) {
        this.quality = quality;
    }
    
    public String getYear() {
        return year;
    }
    
    public void setYear(String year) {
        this.year = year;
    }
    

}
