package com.self.mapreduce.define_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/11 0011-上午 11:00
 */
public class FlowCountSortMapper extends Mapper<LongWritable, Text, FBean, Text> {
    // 这里Key为Bean，因为Hadoop默认对Key进行排序，实现默认Key的Comparable接口，重写CompareTo方法
    FBean k = new FBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //    13502468823	7335	110349	117684
        // 获取一行
        String line = value.toString();
        String[] fields = line.split("\t");

        // 封装对象
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);
        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);

        v.set(phoneNum);

        // 写出
        context.write(k, v);
    }
}
