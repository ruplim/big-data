package com.samples.sample6;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<NullWritable, Text, NullWritable, Text>{

	private PriorityQueue<User> followersQueue = new PriorityQueue<User>();
	
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values,Context ctx)
			throws IOException, InterruptedException {
		
		for(Text value: values) {
		     String line = value.toString();
		     String[] data = line.split("\t");
		     Integer followers = Integer.parseInt(data[1]);
		     
		     User user = followersQueue.peek();

			boolean haveMoreFollowers = false;
			if( user != null && followers > user.getFollowers() )
				haveMoreFollowers = true;
			
			if( followersQueue.size() < Constants.TOP_N_RECORDS || haveMoreFollowers ) {
				followersQueue.add(new User(followers, new Text(value)));

				if( followersQueue.size() > Constants.TOP_N_RECORDS)
					followersQueue.remove(user);
			}
		     
		}
        		
		while (!followersQueue.isEmpty()) {
            ctx.write(NullWritable.get(),followersQueue.poll().getRecord());
        }
		
	}
}
