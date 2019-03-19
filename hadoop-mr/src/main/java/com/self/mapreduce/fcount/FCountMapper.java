package com.self.mapreduce.fcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @ author pxz
 * @ date 2019/2/26 0026-下午 1:31
 */
public class FCountMapper extends Mapper<LongWritable, Text, Text, FBean> {
    Text k = new Text();
    FBean fBean = new FBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 13726230503	120.196.100.82	i02.c.aliimg.com			2481	24681	200
        // 读取一行
        String line = value.toString();

        // 切割
        String[] flows = line.split("\t");

        // 封装对象
        k.set(flows[0]);
        int len = flows.length;

        // 13726230503	2481	24681	200000
        long upf = Long.parseLong(flows[len - 3]);
        long downf = Long.parseLong(flows[len - 2]);

        fBean.add(upf, downf);

        // 写出
        context.write(k, fBean);
    }
}
