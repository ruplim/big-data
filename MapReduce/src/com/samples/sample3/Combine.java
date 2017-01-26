package com.samples.sample3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combine extends Reducer<Text, NumPair, Text, NumPair>{

	@Override
	protected void reduce(final Text key, final Iterable<NumPair> values, final Context ctx)
			throws IOException, InterruptedException {
		
		Double hours = 0.0;
        Integer count = 0;

        for (NumPair value : values) {
            hours += value.getFirst().get();
            count += value.getSecond().get();
        };
        
        /* 
         * The output of mapper is input to combiner. The combiner executes on the same node as mapper.
         * Just add hours and count and send to reducer
         * 
         * */
        
        ctx.write(key, new NumPair(hours, count));
        
	}

	

}
