package com.qiya.flink.demo.kafka;

import java.util.List;
import java.util.Random;

import com.qiya.flink.demo.apitest.beans.CommodityInfoBase;

public class CommoditySaleTest {

	public static void main(String[] args) {
		CommoditySale ksfSale = new CommoditySale(CommodityInfoBase.KANGSHIFU);
		CommoditySale tySale = new CommoditySale(CommodityInfoBase.TONGYI);
		Thread ksfThread = new Thread(ksfSale);
		Thread tyThread = new Thread(tySale);
		ksfThread.start();
		tyThread.start();
	}
}

class CommoditySale implements Runnable {

	private String brand;

	public CommoditySale(String brand) {
		super();
		this.brand = brand;
	}

	@Override
	public void run() {
		int i = 0;
		while (i < 10000) {
			List<String> goodsList = CommodityInfoBase.getCommodityMap().get(this.brand);
			Random random = new Random();
			int index = random.nextInt(goodsList.size());
			int count = random.nextInt(20) + 1;
			StringBuffer sb = new StringBuffer();
			sb.append(this.brand).append(",").append(goodsList.get(index)).append(",").append(count).append(",")
					.append(System.currentTimeMillis());
			KafkaProducerUtils.sendKafkaMessage(sb.toString());
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			i++;
		}
	}
}
