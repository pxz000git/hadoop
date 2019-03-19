package com.self.mapreduce.reduce_join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @ author pxz
 * @ date 2019/3/15 0015-下午 2:59
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    private ArrayList<TableBean> orderBeans;

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 准备要存储对象的集合
        orderBeans = new ArrayList<>();

        // 准备bean对象
        TableBean pdBean = new TableBean();

        for (TableBean bean : values) {
            if (Commons.getTableOrder().equals(bean.getFlag())) {
                TableBean tmp = new TableBean();
                try {
                    // 如果直接写出，只是写出引用
                    BeanUtils.copyProperties(tmp, bean);
                    orderBeans.add(tmp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        //    拼接表
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());

            // 写出数据
            context.write(orderBean, NullWritable.get());
        }

    }
}
