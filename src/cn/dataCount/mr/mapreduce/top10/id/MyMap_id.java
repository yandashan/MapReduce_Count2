package cn.dataCount.mr.mapreduce.top10.id;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 统计最受欢迎的Top10课程
 * 
 * @author Lenovo
 *
 */
public class MyMap_id extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String[] split = line.split(",");

		context.write(new Text(split[5]), new IntWritable(1));

	}

}
