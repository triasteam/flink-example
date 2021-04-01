package com.qiya.flink.demo.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest1Base {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		String inputPath = "D:\\workspace\\flink-demo\\src\\main\\resources\\sensor.txt";

		DataStream<String> inputStream = env.readTextFile(inputPath);

		// 1.map 输出字符串长度
		DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
			@Override
			public Integer map(String value) throws Exception {
				return value.length();
			}

		});

		// 2.flatmap,按逗号分字段
		DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				String[] fields = value.split(",");
				for (String str : fields) {
					out.collect(str);
				}
			}
		});

		// 3.filter,筛选
		DataStream<String> filterMapStream = inputStream.filter(new FilterFunction<String>(){

			@Override
			public boolean filter(String value) throws Exception {
				return value.startsWith("sensor_1");
			}
		});
		
		
		// 打印输出
		mapStream.print();
		flatMapStream.print();
		filterMapStream.print();

		env.execute();
	}
}
