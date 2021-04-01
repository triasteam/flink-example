package com.qiya.flink.demo.apitest.source;

import java.util.Arrays;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import com.qiya.flink.demo.apitest.beans.SensorReading;

public class SourceTest1Collection {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// 从集合中获取数据
		DataSet<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(
				new SensorReading("sensor1", 1617181585537l, 14.6), new SensorReading("sensor2", 1617181585538l, 13.2),
				new SensorReading("sensor5", 1617181585540l, 24.5), new SensorReading("sensor7", 1617181585535l, 4.6),
				new SensorReading("sensor3", 1617181585536l, 8.6), new SensorReading("sensor4", 1617181585517l, 7.9)));
		
		DataSet<Integer> integerDataStream = env.fromElements(123,23,567,1634353,123123);
		
		dataStreamSource.print("data");
		
		integerDataStream.print("int");
		
		env.execute();
	}
}
