package com.self.mapreduce.kv_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/27 0027-下午 5:25
 */
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 封装对象

        // 写出
        context.write(key, v);
    }
}
