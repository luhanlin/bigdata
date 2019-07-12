package com.luhanlin.spark.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Iterator;
import java.util.Map;


public class HiveConf {

    //private static String DEFUALT_CONFIG = "spark/hive/hive-server-config";
    private static HiveConf hiveConf;
    private static HiveContext hiveContext;

    private HiveConf(){

    }

    public static HiveConf getHiveConf(){
        if(hiveConf==null){
            synchronized (HiveConf.class){
                if(hiveConf==null){
                    hiveConf=new  HiveConf();
                }
            }
        }
        return hiveConf;
    }

    public static HiveContext getHiveContext(SparkContext sparkContext){
        if(hiveContext==null){
            synchronized (HiveConf.class){
                if(hiveContext==null){
                    hiveContext = new  HiveContext(sparkContext);
                    Configuration conf = new Configuration();
                    conf.addResource("spark/hive/hive-site.xml");
                    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, String> next = iterator.next();
                        hiveContext.setConf(next.getKey(), next.getValue());
                    }
                    hiveContext.setConf("spark.sql.parquet.mergeSchema", "true");
                }
            }
        }
        return hiveContext;
    }



}
