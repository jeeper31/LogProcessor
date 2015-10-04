package com.gaddieind.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegxInputFormat extends InputFormat<LongWritable, Text>{

    private String regex = "(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2},\\d{3})";
    private Pattern pattern = Pattern.compile(regex);
    private TextInputFormat textIF=new TextInputFormat();


    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
            InterruptedException {

        return textIF.getSplits(context);
    }


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {

        RegexRecordReader reader = new RegexRecordReader();

        if (pattern == null) {
            throw new IllegalStateException(
                    "No pattern specified - unable to create record reader");
        }

        reader.setPattern(pattern);
        return reader;
    }


    public static class RegexRecordReader extends RecordReader<LongWritable, Text> {

        private LineRecordReader lineRecordReader = new LineRecordReader();
        private Pattern pattern;
        Text value = new Text();

        StringBuilder builder;

        public void setPattern(Pattern pattern2) {
            pattern=pattern2;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            lineRecordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            while(lineRecordReader.nextKeyValue()) {
                Matcher matcher;

                final String line = lineRecordReader.getCurrentValue().toString();
                matcher = pattern.matcher(line);

                if (matcher.find()) {
                    if(builder != null) {
                        value = new Text(builder.toString());
                        builder = new StringBuilder(line);
                        return true;
                    }
                    builder = new StringBuilder(line);
                }  else{
                    if(builder == null){
                        return false;
                    }
                    builder.append(line);
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return lineRecordReader.getCurrentKey();
        }

        @Override
        public Text getCurrentValue() throws IOException,
                InterruptedException {
            //System.out.print(value);
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    }
}
