package com.self.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/28 0028-下午 5:02
 */
public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\IdeaWorkspace\\map_reduce\\input\\inputformat", "D:\\IdeaWorkspace\\map_reduce\\output2"};
        Configuration conf = new Configuration();
        //1 获取Job对象
        Job job = Job.getInstance(conf);

        //2 设置jar存储位置
        job.setJarByClass(SequenceFileDriver.class);

        //3 关联Map和Reduce类
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //4 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //5 设置最终输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置输入的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        // 设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7 提交Job
        //job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
