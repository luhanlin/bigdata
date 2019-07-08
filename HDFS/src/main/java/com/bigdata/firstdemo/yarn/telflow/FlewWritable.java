package com.bigdata.firstdemo.yarn.telflow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <类详细描述>
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/24 23:20]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class FlewWritable implements WritableComparable<FlewWritable> {

    private long flewIn;
    private long flewOut;
    private long flewSum;

    public FlewWritable() {
    }

    public FlewWritable(long flewIn, long flewOut) {
        this.flewIn = flewIn;
        this.flewOut = flewOut;
        this.flewSum = this.flewIn + this.flewOut;
    }

    public long getFlewIn() {
        return flewIn;
    }

    public long getFlewOut() {
        return flewOut;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(flewIn);
        dataOutput.writeLong(flewOut);
        dataOutput.writeLong(flewSum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flewIn = dataInput.readLong();
        flewOut = dataInput.readLong();
        flewSum = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "flewIn=" + flewIn +
                "\t flewOut=" + flewOut +
                "\t flewSum=" + flewSum;
    }

    @Override
    public int compareTo(FlewWritable flewWritable) {
        // 倒序
        return this.flewSum >= flewWritable.flewSum ? -1 : 1;
    }
}
