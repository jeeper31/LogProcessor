package com.gaddieind.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by agaddie on 10/3/2015.
 */
public class ExceptionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String stacktrace = new String(value.getBytes());
        int startOfFailingClass = StringUtils.indexOf(stacktrace, "(");
        int endOfFailingClass = StringUtils.indexOf(stacktrace, ")");

        if(startOfFailingClass > 0 && endOfFailingClass > 0) {
            String failingClassInfo = stacktrace.substring(startOfFailingClass + 1, endOfFailingClass);
            context.write(new Text(failingClassInfo), new DoubleWritable(1));
        }
    }
}
