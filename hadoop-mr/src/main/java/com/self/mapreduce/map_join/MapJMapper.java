package com.self.mapreduce.map_join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @ author pxz
 * @ date 2019/3/18 0018-下午 1:37
 */
public class MapJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    HashMap<String, String> hmap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        // 缓存小表
        URI[] files = context.getCacheFiles();
        String path = files[0].getPath();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //01	小米
            // 切割
            String[] fields = line.split("\t");

            // 封装到Hashmap
            hmap.put(fields[0], fields[1]);
        }
        // 关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1001	01	1
        // 从order表取值
        String line = value.toString();
        String[] fields = line.split("\t");

        String id = fields[1];
        String pname = hmap.get(id);

        // 拼接
        line = line + "\t" + pname;

        k.set(line);

        context.write(k, NullWritable.get());
    }
}
