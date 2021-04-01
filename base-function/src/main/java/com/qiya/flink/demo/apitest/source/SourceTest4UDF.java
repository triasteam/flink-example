package com.qiya.flink.demo.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.qiya.flink.demo.apitest.MySensorSource;
import com.qiya.flink.demo.apitest.beans.SensorReading;

public class SourceTest4UDF {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

		dataStream.print();

		env.execute();
	}

}
