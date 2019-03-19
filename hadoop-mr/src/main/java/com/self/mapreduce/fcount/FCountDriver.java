package com.self.mapreduce.fcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/2/26 0026-下午 2:00
 */
public class FCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\IdeaWorkspace\\map_reduce\\input_phone",
                "D:\\IdeaWorkspace\\map_reduce\\output_phone"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FCountDriver.class);
        job.setMapperClass(FCountMapper.class);
        job.setReducerClass(FCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
