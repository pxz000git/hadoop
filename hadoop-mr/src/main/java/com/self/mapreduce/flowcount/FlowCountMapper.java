package com.self.mapreduce.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/25 0025-下午 5:38
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com        	2481    24681   200
        // 获取一行
        String line = value.toString();

        // 切割
        String[] fields = line.split("\t");

        // 封装对象
        k.set(fields[0]);
        int length = fields.length;
        long upflow = Long.parseLong(fields[length - 3]);
        long downflow = Long.parseLong(fields[length - 2]);

        //v.setUpFlow(upflow);
        //v.setDownFlow(downflow);
        v.set(upflow, downflow);

        // 写出
        context.write(k, v);
    }
}
