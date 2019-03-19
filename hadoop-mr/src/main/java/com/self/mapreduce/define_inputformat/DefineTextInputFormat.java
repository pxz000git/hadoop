package com.self.mapreduce.define_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/1 0001-下午 3:47
 */
public class DefineTextInputFormat extends RecordReader<Text, BytesWritable> {
    FileSplit split;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    Configuration conf = new Configuration();
    boolean isProcess = true;


    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProcess) {

            Path path = split.getPath();
            long len = split.getLength();
            byte[] buf = new byte[(int) len];

            // 获取fs对象
            FileSystem fs = path.getFileSystem(conf);

            // 获取输入流
            FSDataInputStream fis = fs.open(path);

            // 拷贝
            IOUtils.readFully(fis, buf, 0, buf.length);

            // 封装 k
            k.set(String.valueOf(path));

            // 封装 v
            v.set(buf, 0, buf.length);

            // 关闭资源
            IOUtils.closeStream(fis);

            isProcess = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
