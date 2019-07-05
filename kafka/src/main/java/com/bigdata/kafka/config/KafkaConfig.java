package com.bigdata.kafka.config;

import com.bigdata.common.config.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class KafkaConfig {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConfig.class);

	private static final String DEFAULT_CONFIG_PATH = "kafka/kafka-server-config.properties";

	private volatile static KafkaConfig kafkaConfig = null;

	private final KafkaProducer config;

    private Properties properties;

	private KafkaConfig() throws IOException{

		try {
			properties = ConfigUtil.getInstance().getProperties(DEFAULT_CONFIG_PATH);
		} catch (Exception e) {
			IOException ioException = new IOException();
			ioException.addSuppressed(e);
			throw ioException;
		}

		config = new KafkaProducer(properties);
	}

	public static KafkaConfig getInstance(){
		
		if(kafkaConfig == null){
			synchronized (KafkaConfig.class) {
				if(kafkaConfig == null){
					try {
						kafkaConfig = new KafkaConfig();
					} catch (IOException e) {
						LOG.error("实例化kafkaConfig失败", e);
					}
				}
			}
		}
		return kafkaConfig;
	}
	
    public KafkaProducer getProducer(){
    	return config;
    }
    
    public static String nowStr(){
    	
    	return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format( new Date() );
    }
    
}
