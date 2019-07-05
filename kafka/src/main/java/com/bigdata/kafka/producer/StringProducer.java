package com.bigdata.kafka.producer;


import com.bigdata.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringProducer {
    private static final Logger LOG = LoggerFactory.getLogger(StringProducer.class);
    private static int threadSize = 6;

    /**
     * 生产单条消息
     * @param topic
     * @param record
     */
    public static void producer(String topic,String record){
        KafkaProducer producer = KafkaConfig.getInstance().getProducer();
        ProducerRecord<String, String> keyedMessage = new ProducerRecord<>(topic, record);
        producer.send(keyedMessage);
        LOG.info("发送数据"+record+"到kafka成功");
        producer.close();
    }

}
