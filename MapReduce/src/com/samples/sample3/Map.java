package com.samples.sample3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, NumPair>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String data = value.toString();
		
		String[] cols = data.split(",");
		
		String maritialStatus = cols[5];
		Double hours = Double.valueOf(cols[12].trim());
		
		context.write(new Text(maritialStatus), new NumPair(hours,1));
		
	}
	
	
	
	

}
