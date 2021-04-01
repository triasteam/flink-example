package com.qiya.userBehaviorAnalysis;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import com.qiya.userBehaviorAnalysis.pojo.CommodityInfo;
import com.qiya.userBehaviorAnalysis.pojo.ItemViewCount;

public class AnalysisApplication {
	public static void main(String[] args) throws Exception {
		// 1.创建执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// kafka 配置
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.50.102:9092");
		properties.setProperty("group.id", "consumer-group");
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("auto.offset.reset", "latest");

		// 2.读取数据，创建dataStream
		DataStream<String> inputStream = env
				.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

		// 3.转换为pojo，分配时间戳和watermark
		DataStream<CommodityInfo> dataStream = inputStream.map(line -> {
			String[] fields = line.split(",");
			return new CommodityInfo(fields[0], fields[1], Long.valueOf(fields[2]), Long.valueOf(fields[3]));
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommodityInfo>() {

			@Override
			public long extractAscendingTimestamp(CommodityInfo element) {
				// TODO Auto-generated method stub
				return element.getTimestamp();
			}
		});

		// 4.分组开窗聚合，得到每个窗口内各个商品的count值
		DataStream<ItemViewCount> windowAggStream = dataStream.filter(data -> "康师傅".equals(data.getBrand()))// 过滤康师傅品牌的数据
				.keyBy("itemName")// 按照产品分组
				.timeWindow(Time.minutes(2), Time.seconds(30)) // 开滑窗
				.aggregate(new ItemCountAgg(), new WindowItemCountResult());

		// 5.收集统一窗口的所有商品count数据，排序输出top n
		DataStream<String> resultStream = windowAggStream.keyBy("windowEnd")// 按照窗口分组
				.process(new TopNHotItems(5))// 用自定义处理函数排序取前5
		;

		resultStream.print();
		env.execute("hot commodity analysis");
	}

	public static class ItemCountAgg implements AggregateFunction<CommodityInfo, Long, Long> {

		@Override
		public Long createAccumulator() {
			return 0l;
		}

		@Override
		public Long add(CommodityInfo value, Long accumulator) {
			return accumulator + 1;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

	public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
		@Override
		public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out)
				throws Exception {
			String itemName = key.getField(0);
			Long windowEnd = window.getEnd();
			Long count = input.iterator().next();
			out.collect(new ItemViewCount(itemName, windowEnd, count));
		}
	}

	public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

		private Integer topSize;

		public TopNHotItems(Integer topSize) {
			super();
			this.topSize = topSize;
		}

		// 定义列表状态，保存当前窗口内所有输出的ItemViewCount
		ListState<ItemViewCount> itemViewCountListState;

		@Override
		public void open(Configuration parameters) throws Exception {
			itemViewCountListState = getRuntimeContext()
					.getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
		}

		@Override
		public void processElement(ItemViewCount value, KeyedProcessFunction<Tuple, ItemViewCount, String>.Context ctx,
				Collector<String> out) throws Exception {
			// 每来一条数据，存入list中，并注册定时器
			itemViewCountListState.add(value);
			ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 50);
		}

		@Override
		public void onTimer(long timestamp, KeyedProcessFunction<Tuple, ItemViewCount, String>.OnTimerContext ctx,
				Collector<String> out) throws Exception {
			// 定时器出发，当前已收集数据排序输出
			ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get());
			itemViewCounts.sort(new Comparator<ItemViewCount>() {

				@Override
				public int compare(ItemViewCount o1, ItemViewCount o2) {
					if (o1.getCount() == o2.getCount()) {
						return 0;
					}
					return o2.getCount() > o1.getCount() ? 1 : -1;
				}
			});
			// 将排名信息格式化string，方便输出
			StringBuffer result = new StringBuffer();
			result.append("======================================");
			result.append("窗口结束时间：").append(new Timestamp(timestamp - 50)).append("\n");

			// 遍历列表，取topn输出
			for (int i = 1; i <= Math.min(topSize, itemViewCounts.size()); i++) {
				ItemViewCount currentItemViewCount = itemViewCounts.get(i);
				result.append("NO ").append(i).append(":").append(" 商品名称 = ").append(currentItemViewCount.getItemName())
						.append(" 商品销量 = ").append(currentItemViewCount.getCount()).append("\n");
			}
			result.append("======================================\n\n");
			out.collect(result.toString());
		}

	}
}
