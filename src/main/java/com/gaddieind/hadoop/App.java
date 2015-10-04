package com.gaddieind.hadoop;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by agaddie on 10/3/2015.
 */
public class App {

    static final String KEYSPACE = "itcs";
    static final String OUTPUT_COLUMN_FAMILY = "error_types";

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
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ExceptionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ExceptionCassandraReducer.class);
        job.setOutputFormatClass(CqlOutputFormat.class);

        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
        job.getConfiguration().set("row_key", "word,sum");
        String query = "UPDATE " + KEYSPACE + "." + OUTPUT_COLUMN_FAMILY + " SET count_num = ? ";
        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);

        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "swcloudtest");
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
