package com.self.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @ author pxz
 * @ date 2019/3/18 0018-下午 4:44
 */
public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 压缩
        compress("hadoop-mr/input/compress/log.txt", "org.apache.hadoop.io.compress.BZip2Codec");
        //compress("input/compress/log.txt", "org.apache.hadoop.io.compress.GzipCodec");
        //compress("input/compress/log.txt", "org.apache.hadoop.io.compress.DefaultCodec");//默认

        // 解压
        //decompress("input/compress/log.txt.deflate");//默认
    }

    private static void decompress(String filePath) throws IOException {
        // 校验是否能解压
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filePath));
        if (codec == null) {
            System.out.println("can not be compress");
            return;
        }

        // 获取输入流
        FileInputStream inputStream = new FileInputStream(new File(filePath));
        CompressionInputStream codecInputStream = codec.createInputStream(inputStream);

        // 获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File(filePath + ".decode"));//.decode只是解压的标识，随便写

        // 流的对拷
        IOUtils.copyBytes(codecInputStream, outputStream, 1024 * 1024, false);

        // 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(codecInputStream);
        IOUtils.closeStream(inputStream);
    }

    private static void compress(String filePath, String method) throws IOException, ClassNotFoundException {
        /*
        压缩
         */

        // 获取输入流
        FileInputStream inputStream = new FileInputStream(new File(filePath));
        Class<?> codecClass = Class.forName(method);

        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

        // 获取输出流(需要编码方式的扩展名)
        FileOutputStream outputStream = new FileOutputStream(new File(filePath + codec.getDefaultExtension()));

        // 压缩（输出文件）
        CompressionOutputStream codecOutputStream = codec.createOutputStream(outputStream);

        //流的对拷
        IOUtils.copyBytes(inputStream, codecOutputStream, 1024 * 1024, false);

        // 关闭资源
        IOUtils.closeStream(codecOutputStream);
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
    }

}
