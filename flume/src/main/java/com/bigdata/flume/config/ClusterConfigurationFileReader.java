package com.bigdata.flume.config;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class ClusterConfigurationFileReader implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigurationFileReader.class);
	private static final String DEFAULT_CONFIG_PATH = "flume/cluster.properties";
	private static ClusterConfigurationFileReader reader;
	private static CompositeConfiguration config;

	private ClusterConfigurationFileReader() {

		LOG.info("加载配置文件 " + DEFAULT_CONFIG_PATH);
		config = new CompositeConfiguration();
		loadConfig(DEFAULT_CONFIG_PATH);
		LOG.info("加载配置文件成功。 ");
	}


	private void loadConfig(String path) {
		try {
			config.addConfiguration(new PropertiesConfiguration(path));
		} catch (ConfigurationException e) {
			LOG.error("加载配置文件 " + path + "失败", e);
		}
	}

	public static ClusterConfigurationFileReader getInstance() {
		if (reader == null) {
			synchronized (ClusterConfigurationFileReader.class) {
				if (reader == null) {
					reader = new ClusterConfigurationFileReader();
				}
			}
		}
		return reader;
	}

	public CompositeConfiguration getConfig() {
		return config;
	}

	// 获取key值
	public String getKey(String string) {
		String string2 = config.getString(string);
		return string2;
	}



	// 获取key值
	public int getKeyInt(String string) {
		int int2 = config.getInt(string);
		return int2;
	}

	// 获取所有字段
	public List<String> getFields(String key) {
		List<String> list = new ArrayList<>();
		config.getList(key).forEach(x->{
			list.add(x+"");
		});
		return list;
	}

	// 获取DateType字段索引
	public int get_index(String dataType, String field) {
		List<String> list = new ArrayList<>();
		config.getList(dataType).forEach(x->{
			list.add(x+"");
		});
		int indexOf = list.indexOf(field);
		return indexOf + 1;
	}

    public static void main(String[] args){
		ClusterConfigurationFileReader reader = ClusterConfigurationFileReader.getInstance();
		String key = reader.getKey("100");
		System.out.println(key);
	}
}
