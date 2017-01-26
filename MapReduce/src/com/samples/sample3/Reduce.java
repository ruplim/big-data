package com.samples.sample3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, NumPair, Text, DoubleWritable> {

    @Override
    public void reduce(final Text key, final Iterable<NumPair> values, final Context context)
            throws IOException, InterruptedException {
        
    	Double hours = 0.0;
        Integer count = 0;

        for (NumPair value : values) {
            hours += value.getFirst().get();
            count += value.getSecond().get();
        };

        Double avg = hours / count;

        context.write(key, new DoubleWritable(avg));
    }
}
