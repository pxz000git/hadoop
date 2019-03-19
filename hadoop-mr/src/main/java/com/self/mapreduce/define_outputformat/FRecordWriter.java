package com.self.mapreduce.define_outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/15 0015-下午 12:49
 */
public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosbaidu;
    FSDataOutputStream fosother;

    public FRecordWriter(TaskAttemptContext job) {

        try {
            // 获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());

            // 创建输出到baidu.log的 输出流
            fosbaidu = fs.create(new Path("d:/baidu/baidu.log"));

            // 创建输出到other.log的 输出流
            fosother = fs.create(new Path("d:/other/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断key当中是否有baidu，如果有，写出到baidu.log，如果没有写出到other.log
        String key_str = key.toString();
        if (key_str.contains("baidu")) {
            fosbaidu.write((key_str + System.getProperty("line.separator")).getBytes());
        } else {
            fosother.write((key_str + System.getProperty("line.separator")).getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fosother);
        IOUtils.closeStream(fosbaidu);
    }
}
