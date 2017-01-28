package com.samples.sample4;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FilterData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("FilterData");
		job.setJarByClass(FilterData.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		Path inputPath = new Path("/home/training/workspace/MapReduce/input/sample4/orders.txt");
		Path outputPath = new Path("/home/training/workspace/MapReduce/output/sample4");
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job.waitForCompletion(true) ? 0 : 1;
		
		
	}
	
	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new FilterData(), args);
		System.exit(exitCode);
		
	}

}
