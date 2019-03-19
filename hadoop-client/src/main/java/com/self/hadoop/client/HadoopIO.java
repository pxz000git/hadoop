package com.self.hadoop.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @ author pxz
 * @ date 2019/2/20 0020-下午 4:07
 */
public class HadoopIO {
    //文件上传
    //@SuppressWarnings("resource")
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        //把本地d盘上的xxx.txt文件上传到HDFS根目录
        // 1 获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");

        // 2 获取输入流
        FileInputStream inputStream = new FileInputStream(new File("test_data/hello.txt"));

        // 3 获取输出流
        Path path_dst = new Path("/hello_dst.txt");
        FSDataOutputStream outputStream = fs.create(path_dst);

        // 4 流的对拷
        IOUtils.copyBytes(inputStream, outputStream, configuration);

        // 5 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fs.close();

        System.out.println("over");
    }

    //文件下载
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //把本地d盘上的xxx.txt文件上传到HDFS根目录
        // 1 获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");

        // 2 获取输入流
        Path path_src = new Path("/hello_dst.txt");
        FSDataInputStream inputStream = fs.open(path_src);

        // 3 获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("test_data/a"));

        // 4 流的对拷
        IOUtils.copyBytes(inputStream, outputStream, configuration);

        // 5 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fs.close();

        System.out.println("over");
    }

    //文件定位读取，下载第一个文件block
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        //把本地d盘上的xxx.txt文件上传到HDFS根目录
        // 1 获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");

        // 2 获取输入流
        Path path_src = new Path("/user/pxz/input/hadoop-2.7.2.tar.gz");
        FSDataInputStream inputStream = fs.open(path_src);

        // 3 获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("test_data/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的对拷
        //int len = 0;
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            //while((len=inputStream.read(buf)) != -1){
            //    outputStream.write(len);
            //}
            inputStream.read(buf);
            outputStream.write(buf);
        }

        // 5 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fs.close();

        System.out.println("over");
    }

    //文件定位读取，下载第二个文件block
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        //把本地d盘上的xxx.txt文件上传到HDFS根目录
        // 1 获取对象
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");

        // 2 获取输入流
        Path path_src = new Path("/user/pxz/input/hadoop-2.7.2.tar.gz");
        FSDataInputStream inputStream = fs.open(path_src);
        // 3 设置指定读取的节点
        inputStream.seek(1024 * 1024 * 128);

        // 4 获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("test_data/hadoop-2.7.2.tar.gz.part3"));

        // 5 流的对拷

        IOUtils.copyBytes(inputStream, outputStream, configuration);

        // 6 关闭资源
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
        fs.close();

        System.out.println("over");
    }
}
