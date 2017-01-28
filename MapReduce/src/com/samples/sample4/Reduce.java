package com.samples.sample4;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<NullWritable, Text, NullWritable, Text>{

	@Override
	//identity function - no work for reducer here
	protected void reduce(NullWritable key, Iterable<Text> values, Context ctx)
			throws IOException, InterruptedException {
		
		for(Text value : values) {
			ctx.write(key, value);
		}
	}
}
