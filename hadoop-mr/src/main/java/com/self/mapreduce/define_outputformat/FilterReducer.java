package com.self.mapreduce.define_outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/15 0015-上午 11:53
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //    https://www.baidu.com
        //    https://www.baidu.com

        // 防止有重复的数据
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }

    }
}
