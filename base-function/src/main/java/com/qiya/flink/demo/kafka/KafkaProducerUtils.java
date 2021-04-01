package com.qiya.flink.demo.kafka;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerUtils {

	public static void sendKafkaMessage(String message) {
		Properties properties = new Properties();
		// kafka是服务器地址
		properties.put("bootstrap.servers", "192.168.50.102:9092");
		// 定义消费者组
		properties.put("group.id", "consumer-group");
		// 自动提交（offset）
		properties.put("enable.auto.commit", "true");
		// 自动处理的间隔时间1秒
		properties.put("auto.commit.interval.ms", "1000");
		// key和values的序列化设置
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 2.创建生产者

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		ProducerRecord<String, String> record = new ProducerRecord<>("sensor", "", message);

		try {
			Future future = producer.send(record);
			future.get();// 不关心是否发送成功，则不需要这行。
		} catch (Exception e) {
			e.printStackTrace();// 连接错误、No Leader错误都可以通过重试解决；消息太大这类错误kafkaProducer不会进行任何重试，直接抛出异常
		}
	}
}
