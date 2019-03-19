package com.self.mapreduce.fcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/26 0026-下午 1:46
 */
public class FCountReducer extends Reducer<Text, FBean, Text, FBean> {
    private FBean fBean = new FBean();

    @Override
    protected void reduce(Text key, Iterable<FBean> values, Context context) throws IOException, InterruptedException {
        long upf = 0;
        long downf = 0;
        long sumf = 0;
        // 累加求和
        for (FBean fbean : values) {
            upf += fbean.getUpf();
            downf += fbean.getDownf();
            sumf += fbean.getSumf();
        }
        fBean.setUpf(upf);
        fBean.setDownf(downf);
        fBean.setSumf(sumf);

        // 写出
        context.write(key, fBean);
    }
}
