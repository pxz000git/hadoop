package com.self.mapreduce.define_inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @ author pxz
 * @ date 2019/3/1 0001-下午 3:57
 */
public class DefineInputFormat extends FileInputFormat<Text, BytesWritable> {
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        DefineTextInputFormat defineTextInputFormat = new DefineTextInputFormat();
        defineTextInputFormat.initialize(split, context);
        return defineTextInputFormat;
    }

}
