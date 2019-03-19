package com.self.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/23 0023-上午 12:04
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 1 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }

        IntWritable v = new IntWritable();
        v.set(sum);

        // 2 写出 (key ,n)
        context.write(key, v);

    }
}
