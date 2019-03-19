package com.self.mapreduce.define_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/11 0011-上午 11:09
 */
public class FlowCountSortReducer extends Reducer<FBean, Text, Text, FBean> {
    @Override
    protected void reduce(FBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 在全区内已排好序

        // (7335	110349	117684,  13502468823)
        // (7335	110349	117684,  13873264687)
        // 循环写出,避免总流量相同的情况
        for (Text value : values) {
            System.out.println(String.format("value is：%s,key is:%s", value, key));
            context.write(value, key);
        }

    }
}
