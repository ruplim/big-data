package com.samples.sample5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, NullWritable, Text, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context ctx)
			throws IOException, InterruptedException {
		
		ctx.write(key, NullWritable.get());
		
	}
}
