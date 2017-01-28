package com.samples.sample4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, NullWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line   = value.toString();
		String[] data = line.split("\t");
		
		if("Books".equalsIgnoreCase(data[2])) {
			context.write(NullWritable.get(), value);
		}
	}
	
	

}
