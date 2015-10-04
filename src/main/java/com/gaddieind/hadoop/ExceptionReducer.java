package com.gaddieind.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by agaddie on 10/3/2015.
 */
public class ExceptionReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double total = 0.0;
        for(DoubleWritable value : values){
            total += value.get();
        }

        context.write(key, new DoubleWritable(total));
    }
}
