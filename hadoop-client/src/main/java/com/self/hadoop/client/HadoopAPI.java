package com.self.hadoop.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ author pxz
 * @ date 2019/2/20 0020-下午 1:21
 */
/*
    HDFS 的API操作
 */
public class HadoopAPI {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
    }

    @Test
    public void create_dir() throws IOException, InterruptedException, URISyntaxException {
        /*
        客户端创建目录
         */
        // 1 获取文件系统
        Configuration conf = new Configuration();
        //在VM中设置 -DHADOOP_USER_NAME=pxz

        // 配置在集群上运行
        //conf.set("fs.defaultFS", "hdfs://apache.hadoop04:9000");
        //FileSystem fs = FileSystem.get(conf);

        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), conf, "pxz");
        Path path = new Path("/user/pxz/hadoop_client_test3/");
        // 2 创建目录
        fs.mkdirs(path);
        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }

    //文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException, URISyntaxException, InterruptedException {
        /*
        参数优先级
            参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的默认配置
         */
        Configuration configuration = new Configuration();
        //configuration.set("dfs.replication", "2");
        String username = "pxz";
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, username);

        Path path_src = new Path("test_data/hello.txt");
        Path path_des = new Path("/user/hello3.txt");
        // 2 上传文件
        fs.copyFromLocalFile(path_src, path_des);

        // 3 关闭资源
        fs.close();

        System.out.println("over");


    }

    //文件下载
    @Test
    public void testCopyToLocalFile() throws IOException, URISyntaxException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");
        Path path_des = new Path("D:\\IdeaWorkspace\\hadoop_client\\test_data");
        Path path_src = new Path("/user/hello3.txt");

        //文件下载
        //fs.copyToLocalFile(path_src, path_des);

        //useRawLocalFileSystem:false :有数据安全校验文件*.crc
        fs.copyToLocalFile(false, path_src, path_des, true);

        // 3 关闭资源
        fs.close();
        System.out.println("over");
    }

    //文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");
        Path path_src = new Path("/user/hello3.txt");
        // 2 文件删除
        //目录， true:递归删除
        fs.delete(path_src, true);

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    //文件更名
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");
        Path path_src = new Path("/user/hello.txt");
        Path path_des = new Path("/user/hello_world.txt");

        // 2 文件更名
        fs.rename(path_src, path_des);

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    //文件详情查看
    @Test
    public void testCat() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");
        Path path_src = new Path("/user");

        // 2 文件详情查看,true:目录递归
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path_src, true);
        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            System.out.println(status.getPath());//文件名称
            System.out.println(status.getPermission());//文件权限
            System.out.println(status.getLen());//文件长度
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);//文件块信息
                }
            }
            System.out.println("============华丽分割线==========");
        }
        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    //判断是文件夹还是文件
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://apache.hadoop04:9000"), configuration, "pxz");
        Path path_src = new Path("/user/pxz/input");

        // 2 判断是文件夹还是文件
        FileStatus[] listStatus = fs.listStatus(path_src);
        for (FileStatus fileStatu : listStatus) {
            if (fileStatu.isDirectory()) {
                //文件夹
                System.out.println("d:" + fileStatu.getPath().getName());
            } else {
                //文件
                System.out.println("f:" + fileStatu.getPath().getName());
            }
            System.out.println("===========华丽分割线==========");

        }

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

}
