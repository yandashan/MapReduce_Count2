package cn.dataCount.mr.mapreduce.top10.id;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce_id extends Reducer<Text, IntWritable, Text, IntWritable> {

	// 实例化一个TreeMap用于自动排序
	TreeMap<PageCount, Object> treeMap = new TreeMap<>();

	/**
	 * 不将reduce进行实施发送，而是暂时将它储存在TreeMap中
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		int count = 0;

		for (IntWritable value : values) {
			count += value.get();
		}
		PageCount pageCount = new PageCount();
		pageCount.set(key.toString(), count);

		// 将这个对象放到TreeMap实现自排序
		treeMap.put(pageCount, null);
	}

	/**
	 * 在Reduce执行完Task后执行这个方法，将所有数据全部发送出去
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// 防止代码修改，从job开始时设置输出的次数，默认值为10
		Configuration configuration = context.getConfiguration();
		int topn = configuration.getInt("top10.id", 10);

		// 从TreeMap中拿出排序完成的所有的值
		Set<Entry<PageCount, Object>> entrySet = treeMap.entrySet();

		// 记录输出的次数
		int i = 0;

		// 从treeMap中取出数据，获取key中的page的值课count的值
		for (Entry<PageCount, Object> entry : entrySet) {
			context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
			i++;
			if (i == topn)
				return;
		}
	}

}
