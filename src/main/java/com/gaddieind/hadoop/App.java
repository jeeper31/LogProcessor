package com.gaddieind.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by agaddie on 10/3/2015.
 */
public class App {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: hadoopex <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(App.class);
        job.setJobName("Exception Counter");
        job.setInputFormatClass(RegxInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ExceptionMapper.class);
        job.setReducerClass(ExceptionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
