package com.self.mapreduce.define_partition;

import com.self.mapreduce.flowcount.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/25 0025-下午 5:54
 */
public class FlowCountReducer extends Reducer<Text, FlowBean2, Text, FlowBean2> {
    private FlowBean2 flowBean = new FlowBean2();

    @Override
    protected void reduce(Text key, Iterable<FlowBean2> values, Context context) throws IOException, InterruptedException {
        //13560436666   2481    24681   30000
        //13560436666	1116    954 	20000

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        // 累加求和
        for (FlowBean2 bean : values) {
            sum_upFlow += bean.getUpFlow();
            sum_downFlow += bean.getDownFlow();
        }
        //flowBean.setUpFlow(sum_upFlow);
        //flowBean.setDownFlow(sum_downFlow);
        //flowBean.setSumFlow(sum_upFlow + sum_downFlow);
        flowBean.set(sum_upFlow, sum_downFlow);

        //写出
        context.write(key, flowBean);
    }
}
