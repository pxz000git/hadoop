package com.self.mapreduce.nline_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/27 0027-下午 5:56
 */
public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 累加求和
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }
        // 写出
        v.set(sum);
        context.write(key, v);


    }
}
