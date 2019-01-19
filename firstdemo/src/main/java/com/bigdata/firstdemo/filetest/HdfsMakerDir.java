package com.bigdata.firstdemo.filetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * <类详细描述> hadoop make dir
 *
 * @author luhanlin
 * @version [V_1.0.0, 2019/1/19 23:55]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class HdfsMakerDir {

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME","root");

        // 配置相关参数，制定namenode 地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.134.121:9000");

        // 创建客户端
        FileSystem fileSystem = FileSystem.get(conf);

        /* 创建目录
         * org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException):
         * Permission denied: user=luhanlin, access=WRITE, inode="/":root:supergroup:drwxr-xr-x
         *
         *  首行 添加 System.setProperty("HADOOP_USER_NAME","root");
         */
        fileSystem.mkdirs(new Path("/test01"));

        fileSystem.close();

        System.out.println("successfully");
    }
}
