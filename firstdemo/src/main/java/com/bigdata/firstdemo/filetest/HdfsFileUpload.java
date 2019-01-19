package com.bigdata.firstdemo.filetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * <类详细描述> file upload
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/20 00:27]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class HdfsFileUpload {

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME","root");

        // 配置相关参数，制定namenode 地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.134.121:9000");

        // 创建客户端
        FileSystem client = FileSystem.get(conf);

        FSDataOutputStream outputStream = client.create(new Path("/test01/a.txt"), true);

        // 读取本地文件
        InputStream inputStream = new FileInputStream(new File("E:\\big_data\\test\\jdk-8u102-linux-x64.tar.gz"));

        IOUtils.copyBytes(inputStream,outputStream,1024);

        System.out.println("successfully");
    }
}
