package com.self.mapreduce.reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/15 0015-下午 2:43
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    String name;
    Text k = new Text();
    TableBean tableBean = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //    1001    01  1
        //    01  小米
        String line = value.toString();

        //封装key,value对象
        if (name.startsWith(Commons.getTableOrder())) {
            String[] fields = line.split("\t");
            k.set(fields[1]);
            tableBean.setOrder_id(fields[0]);
            tableBean.setP_id(fields[1]);
            tableBean.setPname("");
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setFlag(Commons.getTableOrder());
        } else {
            String[] fields = line.split("\t");
            k.set(fields[0]);
            tableBean.setOrder_id("");
            tableBean.setP_id(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setAmount(0);
            tableBean.setFlag(Commons.getTableProduct());
        }
        // 写出
        context.write(k, tableBean);
    }
}
