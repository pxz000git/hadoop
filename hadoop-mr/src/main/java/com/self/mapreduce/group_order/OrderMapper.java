package com.self.mapreduce.group_order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/11 0011-下午 3:33
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    // NullWritable:不需要输出value
    // 0000002	Pdt_05	722.4
    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        //封装对象
        k.setOrder_id(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));

        // 写出
        context.write(k, NullWritable.get());
    }
}
