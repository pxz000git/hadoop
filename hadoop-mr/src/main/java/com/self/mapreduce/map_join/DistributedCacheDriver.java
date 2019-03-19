package com.self.mapreduce.map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ author pxz
 * @ date 2019/3/18 0018-下午 1:38
 */
public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        args = new String[]{"input/reduce_map_join/map/map_join", "output"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DistributedCacheDriver.class);
        job.setMapperClass(MapJMapper.class);
        //job.setReducerClass(TableReducer.class);

        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(TableBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 加载缓存数据
        job.setCacheFiles(new URI[]{new URI("file:///D:/IdeaWorkspace/map_reduce/input/reduce_map_join/map/cache/pd.txt")});
        // Map端Join的逻辑不需要Reduce阶段，设置ReduceTask数量为0
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
