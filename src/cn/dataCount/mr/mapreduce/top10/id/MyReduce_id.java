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

	// ʵ����һ��TreeMap�����Զ�����
	TreeMap<PageCount, Object> treeMap = new TreeMap<>();

	/**
	 * ����reduce����ʵʩ���ͣ�������ʱ����������TreeMap��
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

		// ���������ŵ�TreeMapʵ��������
		treeMap.put(pageCount, null);
	}

	/**
	 * ��Reduceִ����Task��ִ���������������������ȫ�����ͳ�ȥ
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// ��ֹ�����޸ģ���job��ʼʱ��������Ĵ�����Ĭ��ֵΪ10
		Configuration configuration = context.getConfiguration();
		int topn = configuration.getInt("top10.id", 10);

		// ��TreeMap���ó�������ɵ����е�ֵ
		Set<Entry<PageCount, Object>> entrySet = treeMap.entrySet();

		// ��¼����Ĵ���
		int i = 0;

		// ��treeMap��ȡ�����ݣ���ȡkey�е�page��ֵ��count��ֵ
		for (Entry<PageCount, Object> entry : entrySet) {
			context.write(new Text(entry.getKey().getPage()), new IntWritable(entry.getKey().getCount()));
			i++;
			if (i == topn)
				return;
		}
	}

}
