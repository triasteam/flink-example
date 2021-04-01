package com.qiya.userBehaviorAnalysis.pojo;

public class ItemViewCount {

	private String itemName;
	private Long windowEnd;
	private Long count;

	public ItemViewCount() {
		super();
	}

	public ItemViewCount(String itemName, Long windowEnd, Long count) {
		super();
		this.itemName = itemName;
		this.windowEnd = windowEnd;
		this.count = count;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public Long getWindowEnd() {
		return windowEnd;
	}

	public void setWindowEnd(Long windowEnd) {
		this.windowEnd = windowEnd;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "ItemViewCount [itemName=" + itemName + ", windowEnd=" + windowEnd + ", count=" + count + "]";
	}

}
