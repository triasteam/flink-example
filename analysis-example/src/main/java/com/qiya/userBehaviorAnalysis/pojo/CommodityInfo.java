package com.qiya.userBehaviorAnalysis.pojo;

public class CommodityInfo {

	private String brand;
	private String itemName;
	private Long count;
	private Long timestamp;

	public CommodityInfo() {
		super();
	}

	public CommodityInfo(String brand, String itemName, Long count, Long timestamp) {
		super();
		this.brand = brand;
		this.itemName = itemName;
		this.count = count;
		this.timestamp = timestamp;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "ItemInfo [brand=" + brand + ", itemName=" + itemName + ", count=" + count + ", timestamp=" + timestamp
				+ "]";
	}

}
