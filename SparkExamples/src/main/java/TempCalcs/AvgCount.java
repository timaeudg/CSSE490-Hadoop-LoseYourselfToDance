package TempCalcs;

import java.io.Serializable;

public class AvgCount implements Serializable{
    
    Integer accumulator;
    Integer count;
    
    public AvgCount(int acc, int cnt){
        this.accumulator = acc;
        this.count = cnt;
    }

    public Integer getAccumulator() {
        return accumulator;
    }

    public Integer getCount() {
        return count;
    }

    public void setAccumulator(Integer accumulator) {
        this.accumulator = accumulator;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
    
    public double avg(){
        return new Double(this.accumulator) / new Double(this.count);
    }

}
