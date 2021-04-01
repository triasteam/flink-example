package com.qiya.flink.demo.apitest.count.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
//		String inputPath = "D:\\workspace\\flink-demo\\src\\main\\resources\\hello.txt";
//		DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);

		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		String host = parameterTool.get("host");
		int port = parameterTool.getInt("port");
//		System.out.println(host+port);
		// 从socket文本流读取数据
		DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);
		
		// 基于数据流进行转换计算
		SingleOutputStreamOperator<Tuple2<String, Integer>> resultset = inputDataStream.flatMap(new MyFlatMapper())
				.keyBy(0).sum(1);

		resultset.print();

		// 执行任务
		env.execute();

	}

	public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			// 按空格分词
			String[] words = value.split(" ");
			// 遍历所有word,包成二元组输出
			for (String str : words) {
				out.collect(new Tuple2<>(str, 1));
			}
		}

	}
}
