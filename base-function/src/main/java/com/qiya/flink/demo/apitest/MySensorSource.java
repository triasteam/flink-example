package com.qiya.flink.demo.apitest;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.qiya.flink.demo.apitest.beans.SensorReading;

public class MySensorSource implements SourceFunction<SensorReading> {

	// 定义一个标志位，用来控制数据的产生
	private boolean running = true;

	@Override
	public void run(SourceContext<SensorReading> ctx) throws Exception {
		Random random = new Random();

		Map<String, Double> sensorTempMap = new HashMap<String, Double>();
		for (int i = 1; i <= 10; i++) {
			sensorTempMap.put("sensor_" + i, 60 + random.nextGaussian() * 20);
		}
		while (running) {
			for (String sensorId : sensorTempMap.keySet()) {
				Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian() * 0.5;
				sensorTempMap.put(sensorId, newTemp);
				ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));
			}
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub
		running = false;
	}

}
