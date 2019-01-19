package com.bigdata.firstdemo.filetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * <类详细描述> file download
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/20 00:33]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class HdfsFileDownload {

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME","root");

        // 配置相关参数，制定namenode 地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.134.121:9000");

        // 创建客户端
        FileSystem client = FileSystem.get(conf);

        FSDataInputStream inputStream = client.open(new Path("/test01/a.txt"));

        // 读取本地文件
        OutputStream outputStream = new FileOutputStream(new File("E:\\big_data\\test\\test.zip"));

        IOUtils.copyBytes(inputStream,outputStream,1024);
    }

}
