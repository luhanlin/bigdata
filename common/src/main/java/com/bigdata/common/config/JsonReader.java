package com.bigdata.common.config;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author:
 * @description:
 * @Date:Created in 2018-05-18 09:33
 */
public class JsonReader {
    private static Logger LOG = LoggerFactory.getLogger(JsonReader.class);

    public static String readJson(String json_path){
        JsonReader jsonReader = new JsonReader();
        return jsonReader.getJson(json_path);
    }

    private String getJson(String json_path){
        String jsonStr = "";
        try {
            String path = getClass().getClassLoader().getResource(json_path).toString();
            path = path.replace("\\", "/");
            if (path.contains(":")) {
                path = path.replace("file:/","");
            }
            jsonStr = FileUtils.readFileToString(new File(path), "UTF-8");
            LOG.error("读取json文件{}成功",path);
        } catch (Exception e) {
            LOG.error("读取json文件失败",e);
        }
        return jsonStr;
    }
}
