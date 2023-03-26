package pairrdd.reducebykey.houseprice;

import java.io.Serializable;

/**
 * @author JayendraB
 * Created on 12/12/21
 */
public class AvgCount implements Serializable {

    private int count;

    private double total;

    public AvgCount(int count, double total) {
        this.count = count;
        this.total = total;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    @Override
    public String toString() {
        return "AvgCount{" +
                "count=" + count +
                ", total=" + total +
                '}';
    }
}
