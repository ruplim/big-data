package com.samples.sample6;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, NullWritable, Text> {

	private PriorityQueue<User> followersQueue = new PriorityQueue<User>();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] data = line.split("\t");
		
		Integer followers = Integer.parseInt(data[1]);
		
		//peek :: Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
		User user = followersQueue.peek();

		boolean haveMoreFollowers = false;
		if( user != null && followers > user.getFollowers() )
			haveMoreFollowers = true;
		
		if( followersQueue.size() < Constants.TOP_N_RECORDS || haveMoreFollowers ) {
			followersQueue.add(new User(followers, new Text(value)));
			
			if( followersQueue.size() > Constants.TOP_N_RECORDS)
                //poll :: Retrieves and removes the head of this queue, or returns null if this queue is empty.
				//in this case we are just removing
				followersQueue.poll();
		}		
	}

	@Override
	//cleanup() method is called at the end of map task on each node
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		while (!followersQueue.isEmpty()) {
            context.write(NullWritable.get(), followersQueue.poll().getRecord());
        }
	}
	
}
