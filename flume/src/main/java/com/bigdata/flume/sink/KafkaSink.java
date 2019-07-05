package com.bigdata.flume.sink;


import com.bigdata.kafka.producer.StringProducer;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

public class KafkaSink extends AbstractSink implements Configurable {

	private final Logger logger = Logger.getLogger(KafkaSink.class);
	private String[] kafkatopics = null;

	@Override
	public void configure(Context context) {
		kafkatopics = context.getString("kafkatopics").split(",");
	}

	@Override
	public Status process() throws EventDeliveryException {

		logger.info("sink开始执行");
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		try {
			Event event = channel.take();
			if (event == null) {
				transaction.rollback();
				return Status.BACKOFF;
			}
			// 解析记录
			String record = new String(event.getBody());
			// 发送数据到kafka
			try {
				StringProducer.producer(kafkatopics[0],record);
			/*	if(listKeyedMessage.size()>1000){
					logger.info("数据大与10000,推送数据到kafka");
					sendListKeyedMessage();
					logger.info("数据大与10000,推送数据到kafka成功");
				}else if(System.currentTimeMillis()-proTimestamp>=60*1000){
					logger.info("时间间隔大与60,推送数据到kafka");
					sendListKeyedMessage();
					logger.info("时间间隔大与60,推送数据到kafka成功"+listKeyedMessage.size());
				}*/

			} catch (Exception e) {
				logger.error("推送数据到kafka失败" , e);
				throw Throwables.propagate(e);
			}

			transaction.commit();
			return Status.READY;
		} catch (ChannelException e) {
			logger.error(e);
			transaction.rollback();
			return Status.BACKOFF;
		} finally {
			if(transaction != null){
				transaction.close();
			}
		}
	}

	@Override
	public synchronized void stop() {
		super.stop();
	}

}

