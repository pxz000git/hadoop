package com.self.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/23 0023-上午 12:14
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"hadoop-mr/input/inputword", "output"};
        Configuration conf = new Configuration();
        //1 获取Job对象
        Job job = Job.getInstance(conf);

        //2 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        //3 关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5 设置最终输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // CombineTextInputFormat

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
        //job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置4m
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        //虚拟存储切片最大值设置20m
        //CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        //设置ReduceTasks数目
        //job.setNumReduceTasks(2);

        // 输出局部汇总
        //方式一：
        // 关联WordCountCombiner
        //job.setCombinerClass(WordCountCombiner.class);
        //方式二：
        job.setCombinerClass(WordCountReducer.class);

        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交Job
        //job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
