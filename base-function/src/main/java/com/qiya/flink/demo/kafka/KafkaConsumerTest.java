package com.qiya.flink.demo.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerTest {
	public static void main(String[] args) {
		// 1.配置属性值
		Properties properties = new Properties();
		// kafka是服务器地址
		properties.put("bootstrap.servers", "192.168.50.102:9092");
		// 定义消费者组
		properties.put("group.id", "consumer-group");
		// 自动提交（offset）
		properties.put("enable.auto.commit", "true");
		// 自动处理的间隔时间1秒
		properties.put("auto.commit.interval.ms", "1000");
		// key和values的持久化设置
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//2.创建消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //3.订阅消费topic(可以有多个topic)
        kafkaConsumer.subscribe(Arrays.asList("sensor"));
        //4.执行消费的操作
        while (true) {
            //100ms消费一次
            //kafkaConsumer.poll(100)读出来，读到records
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("-----------------");
            //打印偏移量，key，value
                System.out.printf("offset = %d,key = %s, value = %s", record.offset(), record.key(),record.value());
                System.out.println();
            }
        }
	}

}
