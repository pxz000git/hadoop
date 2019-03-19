package com.self.mapreduce.nline_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/27 0027-下午 6:07
 */
public class NLineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\IdeaWorkspace\\map_reduce\\input\\input_nline", "D:\\IdeaWorkspace\\map_reduce\\output"};
        Configuration conf = new Configuration();
        //1 获取Job对象
        Job job = Job.getInstance(conf);



        //2 设置jar存储位置
        job.setJarByClass(NLineDriver.class);

        //3 关联Map和Reduce类
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        //4 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5 设置最终输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置每个切片InputSplit中划分三条记录数
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        // 使用NLineInputFormat处理记录数据
        job.setInputFormatClass(NLineInputFormat.class);

        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交Job
        //job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
