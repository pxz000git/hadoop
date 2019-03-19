package com.self.mapreduce.fcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/26 0026-下午 1:28
 */
public class FBean implements Writable {
    private long upf;
    private long downf;
    private long sumf;

    public FBean() {
        super();
    }

    public long getUpf() {
        return upf;
    }

    public void setUpf(long upf) {
        this.upf = upf;
    }

    public long getDownf() {
        return downf;
    }

    public void setDownf(long downf) {
        this.downf = downf;
    }

    public long getSumf() {
        return sumf;
    }

    public void setSumf(long sumf) {
        this.sumf = sumf;
    }

    public void add(long upf, long downf) {
        this.upf = upf;
        this.downf = downf;
        this.sumf = upf + downf;
    }

    //序列化和反序列化的顺序是一致的
    // 序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upf);
        dataOutput.writeLong(downf);
        dataOutput.writeLong(sumf);

    }

    // 反序列化方法

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upf = dataInput.readLong();
        downf = dataInput.readLong();
        sumf = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upf + "\t" + downf + "\t" + sumf;
    }
}
