package cn.dataCount.mr.mapreduce.top10.id;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmit_id {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("top.10", 10);

		// // 判断输入输出路径
		// if (args == null || args.length < 2) {
		// System.err.println("Parameter
		// Errors!Usage<inputPath...><outputPath>");
		// System.exit(-1);
		// }

		Job job = Job.getInstance(conf);
		job.setJobName("JobSubmit_id");
		job.setJarByClass(JobSubmit_id.class);

		// map设置
		job.setMapperClass(MyMap_id.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reduce设置
		job.setReducerClass(MyReduce_id.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 文件输入路径与输出路径
		FileInputFormat.setInputPaths(job,
				new Path("hdfs://192.168.57.128:9000/MyMapReduce/AccessLogClean/Result/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.57.128:9000/MyMapReduce/MyMapReduce_id/Result"));

		// 任务提示
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag);
		System.exit(flag ? 0 : 1);
	}
}
