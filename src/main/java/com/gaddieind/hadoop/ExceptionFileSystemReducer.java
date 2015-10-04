package com.gaddieind.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by agaddie on 10/3/2015.
 */
public class ExceptionFileSystemReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer total = 0;
        for(IntWritable value : values){
            total += value.get();
        }

        context.write(key, new IntWritable(total));
    }
}
