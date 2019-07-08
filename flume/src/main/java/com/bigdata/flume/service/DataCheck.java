package com.bigdata.flume.service;


import com.alibaba.fastjson.JSON;
import com.bigdata.common.datatype.DataTypeProperties;
import com.bigdata.common.net.HttpRequest;
import com.bigdata.common.time.TimeTranslationUtils;
import com.bigdata.flume.fields.ErrorMapFields;
import com.bigdata.flume.fields.MapFields;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Description: 数据校验
 *
 * @author
 * @version 1.0
 * @date 2016/9/21 16:02
 */

public class DataCheck {

    private final static Logger LOG = Logger.getLogger(DataCheck.class);


    /**
     * 获取数据类型对应的字段  对应的文件
     * 结构为 [ 数据类型1 = [字段1，字段2。。。。]，
     * 数据类型2 = [字段1，字段2。。。。]]
     */
    private static Map<String, ArrayList<String>> dataMap = DataTypeProperties.dataTypeMap;

    /**
     * 数据解析
     *
     * @param line
     * @param fileName
     * @param absoluteFilename
     * @return
     */
    public static Map<String, String> txtParse(String line, String fileName, String absoluteFilename) {

        Map<String, String> map = new HashMap<String, String>();
        String[] fileNames = fileName.split("_");
        String dataType = fileNames[0];
        if (dataMap.containsKey(dataType)) {
            List<String> fields = dataMap.get(dataType.toLowerCase());
            String[] splits = line.split("\t");
            //长度校验
            if (fields.size() == splits.length) {
                //添加公共字段
                map.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
                map.put(MapFields.TABLE, dataType.toLowerCase());
                map.put(MapFields.RKSJ, (System.currentTimeMillis() / 1000) + "");
                map.put(MapFields.FILENAME, fileName);
                map.put(MapFields.ABSOLUTE_FILENAME, absoluteFilename);
                for (int i = 0; i < splits.length; i++) {
                    map.put(fields.get(i), splits[i]);
                }
            } else {
                map = null;
                LOG.error("字段长度不匹配fields"+fields.size()  + "/t" + splits.length);
            }
        } else {
            map = null;
            LOG.error("配置文件中不存在此数据类型");
        }

        return map;
    }


    /**
     * 数据长度校验添加必要字段并转map，将长度不符合的插入ES数据库
     *
     * @param line
     * @param fileName
     * @param absoluteFilename
     * @return
     */
    public static Map<String, String> txtParseAndalidation(String line, String fileName, String absoluteFilename) {

        Map<String, String> map = new HashMap<String, String>();
        Map<String, Object> errorMap = new HashMap<String, Object>();
        //文件名按"_"切分  wechat_source1_1111142.txt
        //wechat 数据类型
        //source1 数据来源
        //1111142  不让文件名相同
        String[] fileNames = fileName.split("_");
        String dataType = fileNames[0];
        String source = fileNames[1];

        if (dataMap.containsKey(dataType)) {
            //获取数据类型字段
           // imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
            List<String> fields = dataMap.get(dataType.toLowerCase());
            String[] splits = line.split("\t");
            //长度校验
            if (fields.size() == splits.length) {
                for (int i = 0; i < splits.length; i++) {
                    map.put(fields.get(i), splits[i]);
                }
                //添加公共字段
                // map.put(SOURCE, source);
                map.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
                map.put(MapFields.TABLE, dataType.toLowerCase());
                map.put(MapFields.RKSJ, (System.currentTimeMillis() / 1000) + "");
                map.put(MapFields.FILENAME, fileName);
                map.put(MapFields.ABSOLUTE_FILENAME, absoluteFilename);

                //数据封装完成  开始进行数据校验
                errorMap = DataValidation.dataValidation(map);

            } else {
                errorMap.put(ErrorMapFields.LENGTH, "字段数不匹配 实际" + fields.size() + "\t" + "结果" + splits.length);
                errorMap.put(ErrorMapFields.LENGTH_ERROR, ErrorMapFields.LENGTH_ERROR_NUM);
                LOG.info("字段数不匹配 实际" + fields.size() + "\t" + "结果" + splits.length);
                map = null;
            }

            //判断数据是否存在错误
            if (null != errorMap && errorMap.size() > 0) {
                LOG.info("errorMap===" + errorMap);
                //addErrorMapES(errorMap, map, fileName, absoluteFilename);
                addErrorMapESByHTTP(errorMap, map, fileName, absoluteFilename);
                map = null;
            }
        } else {
            map = null;
            LOG.error("配置文件中不存在此数据类型");
        }

        return map;
    }






    /**
     *  将错误信息写入ES，方便查错
     * @param errorMap
     * @param map
     * @param fileName
     * @param absoluteFilename
     */
    public static void addErrorMapESByHTTP(Map<String, Object> errorMap, Map<String, String> map, String fileName, String absoluteFilename) {

        String errorType = fileName.split("_")[0];
        errorMap.put(MapFields.TABLE, errorType);
        errorMap.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
        errorMap.put(ErrorMapFields.RECORD, map);
        errorMap.put(ErrorMapFields.FILENAME, fileName);
        errorMap.put(ErrorMapFields.ABSOLUTE_FILENAME, absoluteFilename);
        errorMap.put(ErrorMapFields.RKSJ, TimeTranslationUtils.Date2yyyy_MM_dd_HH_mm_ss());
        String url="http://192.168.247.101:9200/error_recourd/error_recourd/"+ errorMap.get(MapFields.ID).toString();
        String json = JSON.toJSONString(errorMap);
        HttpRequest.sendPost(url,json);
        //HttpRequest.sendPostMessage(url, errorMap);
    }

 /*
    public static void addErrorMapES(Map<String, Object> errorMap, Map<String, String> map, String fileName, String absoluteFilename) {

        String errorType = fileName.split("_")[0];
        errorMap.put(MapFields.TABLE, errorType);
        errorMap.put(MapFields.ID, UUID.randomUUID().toString().replace("-", ""));
        errorMap.put(ErrorMapFields.RECORD, map);
        errorMap.put(ErrorMapFields.FILENAME, fileName);
        errorMap.put(ErrorMapFields.ABSOLUTE_FILENAME, absoluteFilename);
        errorMap.put(ErrorMapFields.RKSJ, TimeTranstationUtils.Date2yyyy_MM_dd_HH_mm_ss());


        TransportClient client = null;
        try {
            LOG.info("开始获取客户端===============================" + errorMap);
            client = ESClientUtils.getClient();
        } catch (Throwable t) {
            if (t instanceof Error) {
                throw (Error)t;
            }
            LOG.error(null,t);
        }
        //JestClient jestClient = JestService.getJestClient();
        //boolean bool = JestService.indexOne(jestClient,TxtConstant.ERROR_INDEX, TxtConstant.ERROR_TYPE,errorMap.get(MapFields.ID).toString(),errorMap);
        LOG.info("开始写入错误数据到ES===============================" + errorMap);
        boolean bool = IndexUtil.putIndexData(TxtConstant.ERROR_INDEX, TxtConstant.ERROR_TYPE, errorMap.get(MapFields.ID).toString(), errorMap,client);
        if(bool){
            LOG.info("写入错误数据到ES===============================" + errorMap);
        }else{
            LOG.info("写入错误数据到ES===============================失败");
        }

    }*/


    public static void main(String[] args) {

    }

}
