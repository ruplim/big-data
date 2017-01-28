package com.samples.sample6;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopN extends Configured implements Tool {
	    
	    @Override
	    public int run(String[] args) throws Exception{
	    
	    	Job job = Job.getInstance(getConf());
	        job.setJobName("TopN");
	        job.setJarByClass(TopN.class);
	        
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	        
	        job.setMapperClass(Map.class);
	        job.setReducerClass(Reduce.class);
	        
	        Path inputFilePath = new Path("/home/training/workspace/MapReduce/input/sample6/socialnetwork.txt");
	        Path outputFilePath = new Path("/home/training/workspace/MapReduce/output/sample6");
	        
	        FileInputFormat.addInputPath(job, inputFilePath);
	        FileOutputFormat.setOutputPath(job, outputFilePath);
	        
	        return job.waitForCompletion(true) ? 0 : 1;
	    }
	    
	    public static void main(String[] args) throws Exception {
	        int exitCode = ToolRunner.run(new TopN(), args);
	        System.exit(exitCode);
	    }
	}

