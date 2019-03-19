package com.self.mapreduce.define_sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @ author pxz
 * @ date 2019/3/11 0011-上午 11:13
 */
// 全排序、分区内排序
public class FlowCountSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\IdeaWorkspace\\map_reduce\\input\\input_sort", "D:\\IdeaWorkspace\\map_reduce\\output2"};
        Configuration conf = new Configuration();
        //1 获取Job对象
        Job job = Job.getInstance(conf);

        //2 设置jar存储位置
        job.setJarByClass(FlowCountSortDriver.class);

        //3 关联Map和Reduce类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        //4 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(FBean.class);
        job.setMapOutputValueClass(Text.class);

        //5 设置最终输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FBean.class);

        // 关联分区
        job.setPartitionerClass(ProvidencePartitioner.class);
        job.setNumReduceTasks(5);

        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交Job
        //job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
