package com.self.mapreduce.nline_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/27 0027-下午 5:53
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();

        // 切割
        String[] words = line.split(" ");

        // 封装对象
        for (String word : words) {
            k.set(word);

            // 写出
            context.write(k, v);
        }
    }
}
