package com.self.mapreduce.group_order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/11 0011-下午 3:40
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //一次遍历后面的key,相同的key才会一起进入到reducer中

        // 直接只输出第一组数据，就是最大的
        //context.write(key, NullWritable.get());

        // 每个id输出价格前3
        int i = 0;
        for (NullWritable value : values) {
            if (i < 3) {
                context.write(key, value);
                i++;
            }
        }

    }
}
