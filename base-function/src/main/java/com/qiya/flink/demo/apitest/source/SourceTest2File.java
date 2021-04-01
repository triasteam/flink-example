package com.qiya.flink.demo.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2File {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String inputPath = "D:\\workspace\\flink-demo\\src\\main\\resources\\sensor.txt";

		DataStream<String> dataStream = env.readTextFile(inputPath);

		dataStream.print();
		
		env.execute();
	}

}
