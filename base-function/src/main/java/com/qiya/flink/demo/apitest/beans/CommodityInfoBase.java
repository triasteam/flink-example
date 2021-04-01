package com.qiya.flink.demo.apitest.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommodityInfoBase {

	public static String KANGSHIFU = "康师傅";

	public static String TONGYI = "统一";

	private static Map<String, List<String>> commodityMap;

	static {
		commodityMap = new HashMap<String, List<String>>();
		List<String> ksf = new ArrayList<String>();
		ksf.add("康师傅红烧牛肉面");
		ksf.add("康师傅冰红茶");
		ksf.add("康师傅老坛酸菜面");
		ksf.add("康师傅矿泉水");
		ksf.add("康师傅绿茶");
		ksf.add("康师傅鲜虾鱼板面");
		ksf.add("康师傅金汤肥牛面");
		ksf.add("康师傅香菇炖鸡面");
		ksf.add("康师傅茉莉清茶");
		ksf.add("康师傅柠檬茶");
		commodityMap.put(KANGSHIFU, ksf);

		List<String> ty = new ArrayList<String>();
		ty.add("统一红烧牛肉面");
		ty.add("统一冰红茶");
		ty.add("统一老坛酸菜面");
		ty.add("统一汤达人日式豚骨拉面");
		ty.add("统一藤椒牛肉面");
		ty.add("统一小浣熊干脆面");
		ty.add("统一那街那巷兰州牛肉面");
		commodityMap.put(TONGYI, ty);
	}

	public static Map<String, List<String>> getCommodityMap() {
		return commodityMap;
	}

}
