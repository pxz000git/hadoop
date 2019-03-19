package com.self.mapreduce.group_order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ author pxz
 * @ date 2019/3/11 0011-下午 3:53
 */
// OrderGroupComparator是在ReducerTask中完成的
public class OrderGroupComparator extends WritableComparator {

    // 创建一个构造将比较对象的类传给父类
    // 父类要知道是比较哪一个类，否则类型转换异常
    // 传值true 则空指针异常
    protected OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //要求只要id相同，就认为是相同的key
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result;
        if (aBean.getOrder_id() > bBean.getOrder_id()) {
            result = 1;
        } else if (aBean.getOrder_id() < bBean.getOrder_id()) {
            result = -1;
        } else {
            result = 0; // 将数据传入到同一个ReduceTask的key
        }
        return result;
    }
}
