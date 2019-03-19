package com.self.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/11 0011-下午 2:39
 */
// 在map输出之后reducer输入之前进行合并[该过程是在内存（环形缓冲圈）中进行]，减少IO
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        // 写出
        context.write(key, v);
    }
}
