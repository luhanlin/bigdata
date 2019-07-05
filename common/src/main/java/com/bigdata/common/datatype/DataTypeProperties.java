
package com.bigdata.common.datatype;

import com.bigdata.common.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * @author:
 * @description:
 * @Date:Created in 2019-04-26 18:28
 */

public class DataTypeProperties {
    private static final Logger logger = LoggerFactory.getLogger(DataTypeProperties.class);

    private static final String DATA_PATH = "common/data-type.properties";

    public static Map<String,ArrayList<String>> dataTypeMap = null;

    static {
        Properties properties = ConfigUtil.getInstance().getProperties(DATA_PATH);
        dataTypeMap = new HashMap<>();
        Set<Object> keys = properties.keySet();
        keys.forEach(key->{
            String[] split = properties.getProperty(key.toString()).split(",");
            dataTypeMap.put(key.toString(),new ArrayList<>(Arrays.asList(split)));
        });
    }

    public static void main(String[] args) {
        Map<String, ArrayList<String>> dataTypeMap = DataTypeProperties.dataTypeMap;
        System.out.println(dataTypeMap.toString());
    }

}

