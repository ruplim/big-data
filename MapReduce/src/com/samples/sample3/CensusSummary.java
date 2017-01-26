package com.samples.sample3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CensusSummary extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("CensusSummary");
		job.setJarByClass(CensusSummary.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NumPair.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combine.class);
		
		Path inputFilePath = new Path("/home/training/workspace/MapReduce/input/sample3/census.txt");
        Path outputFilePath = new Path("/home/training/workspace/MapReduce/output/sample3B");
        
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;
	}
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CensusSummary(), args);
        System.exit(exitCode);
    }
	
	
	
	

}
