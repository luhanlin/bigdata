package com.bigdata.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.bigdata.common.datatype.DataTypeProperties;
import com.bigdata.flume.fields.MapFields;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Description:
 * @author
 * @version 1.0
 * @date 2016/9/21 15:24数据清洗过滤器
 */

public class DataCleanInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(DataCleanInterceptor.class);
    //datatpye.properties
    private static Map<String,ArrayList<String>> dataMap = DataTypeProperties.dataTypeMap;

    /**
     *  初始化
     */
    @Override
    public void initialize() {
    }

    /**
     * 拦截方法。数据解析，封装，数据清洗
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        SimpleEvent eventNew = new SimpleEvent();
        try {
            LOG.info("拦截器Event开始执行");
            Map<String, String> map = parseEvent(event);
            if(map == null){
                return null;
            }
            String lineJson = JSON.toJSONString(map);
            LOG.info("拦截器推送数据到channel:" +lineJson);
            eventNew.setBody(lineJson.getBytes());
        } catch (Exception e) {
            LOG.error(null,e);
        }
        return eventNew;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> list = new ArrayList<Event>();
        for (Event event : events) {
            Event intercept = intercept(event);
            if (intercept != null) {
                list.add(intercept);
            }
        }
        return list;
    }

    @Override
    public void close() {
    }


    /**
     * 数据解析
     * @param event
     * @return
     */
    public static Map<String,String> parseEvent(Event event){
        if (event == null) {
            return null;
        }
        //000000000000000	000000000000000	24.000000	25.000000	aa-aa-aa-aa-aa-aa	bb-bb-bb-bb-bb-bb	32109231	1557305985	andiy	18609765432	judy			1789098763
        String line = new String(event.getBody(), Charsets.UTF_8);
        String filename = event.getHeaders().get(MapFields.FILENAME);
        String absoluteFilename = event.getHeaders().get(MapFields.ABSOLUTE_FILENAME);
        //wechat_source1_1111115.txt
        String[] fileNames = filename.split("_");
        // String转map，并进行数据长度校验，校验错误入ES错误表
        //Map<String, String> map = JZDataCheck.txtParse(type, line, source, filename,absoluteFilename);
        Map<String,String> map = new HashMap<>();
        //000000000000000	000000000000000	24.000000	25.000000	aa-aa-aa-aa-aa-aa	bb-bb-bb-bb-bb-bb	32109231	1557305985	andiy	18609765432	judy			1789098763
        String[] split = line.split("\t");
        //数据类别
        String dataType = fileNames[0];
        //imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
        ArrayList<String> fields = dataMap.get(dataType);
        for (int i = 0; i < split.length; i++) {
            map.put(fields.get(i),split[i]);
        }
        //添加ID
        map.put(MapFields.ID, UUID.randomUUID().toString().replace("-",""));
        map.put(MapFields.TABLE, dataType);
        map.put(MapFields.FILENAME, filename);
        map.put(MapFields.ABSOLUTE_FILENAME, absoluteFilename);
        return map;
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public void configure(Context context) {
        }
        @Override
        public Interceptor build() {
            return new DataCleanInterceptor();
        }
    }
}
