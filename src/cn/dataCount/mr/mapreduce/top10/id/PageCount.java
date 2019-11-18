package cn.dataCount.mr.mapreduce.top10.id;

/**
 * 创建一个实体类，实现一个比较器
 * 
 * @author Lenovo
 *
 */
public class PageCount implements Comparable<PageCount> {

	private String page;
	private int count;

	public void set(String page, int count) {
		this.page = page;
		this.count = count;
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	// 设置比较器，用于降序输出
	@Override
	public int compareTo(PageCount o) {
		return o.getCount() - this.count == 0 ? this.page.compareTo(o.getPage()) : o.getCount() - this.count;
	}

}
