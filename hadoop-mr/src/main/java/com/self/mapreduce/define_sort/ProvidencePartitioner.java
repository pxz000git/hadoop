package com.self.mapreduce.define_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ author pxz
 * @ date 2019/3/11 0011-下午 1:42
 */
public class ProvidencePartitioner extends Partitioner<FBean, Text> {
    @Override
    public int getPartition(FBean fBean, Text text, int numPartitions) {
        // 按照手机号的前3位分区
        String phoneNum = text.toString();
        String prePhoneNum = phoneNum.substring(0, 3);
        int partition = 4;
        if ("136".equals(prePhoneNum)) {
            partition = 3;
        } else if ("137".equals(prePhoneNum)) {
            partition = 2;
        } else if ("138".equals(prePhoneNum)) {
            partition = 1;
        } else if ("139".equals(prePhoneNum)) {
            partition = 0;
        }
        return partition;
    }
}
