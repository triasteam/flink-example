package com.qiya.flink.demo.apitest.count.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理word count
public class WordCount {
	public static void main(String[] args) throws Exception {
		// 创建执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// 从文件中读取数据
		String inputPath = "D:\\workspace\\flink-demo\\src\\main\\resources\\hello.txt";
		DataSource<String> stringDatasource = env.readTextFile(inputPath);

		// 数据处理,按空格粉刺展开，转换成(word,1)进行统计
		DataSet<Tuple2<String,Integer>> resultset = stringDatasource.flatMap(new MyFlatMapper())
		// 按照第一个位置的word分组
		.groupBy(0)
		// 将第二个位置的值求和
		.sum(1);
		resultset.print();
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
