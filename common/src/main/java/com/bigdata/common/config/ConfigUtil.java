package com.bigdata.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {

    private static Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);

    private static ConfigUtil configUtil;

    public static ConfigUtil getInstance(){

        if(configUtil == null){
            configUtil = new ConfigUtil();
        }
        return configUtil;
    }


    public Properties getProperties(String path){
        Properties properties = new Properties();
        try {
            LOG.info("开始加载配置文件" + path);
            InputStream ins = this.getClass().getClassLoader().getResourceAsStream(path);
            properties = new Properties();
            properties.load(ins);
        } catch (IOException e) {
            LOG.info("加载配置文件" + path + "失败");
            LOG.error(null,e);
        }

        LOG.info("加载配置文件" + path + "成功");
        return properties;
    }


    public static void main(String[] args) {
        ConfigUtil instance = ConfigUtil.getInstance();
        Properties properties = instance.getProperties("common/data-type.properties");
        System.out.println(properties);
    }
}
