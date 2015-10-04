package com.gaddieind.hadoop;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by agaddie on 10/4/2015.
 *
 * will output to the following table
 *
 *  create table mykeyspace.error_types (word text, count_num text, PRIMARY KEY(word));
 */
public class ExceptionCassandraReducer extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {

    private Map<String, ByteBuffer> keys;
    private ByteBuffer key;
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
            throws IOException, InterruptedException
    {
        keys = new LinkedHashMap<>();
    }

    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (IntWritable val : values)
            sum += val.get();
        keys.put("word", ByteBufferUtil.bytes(word.toString()));
        context.write(keys, getBindVariables(word, sum));
    }

    private List<ByteBuffer> getBindVariables(Text word, int sum)
    {
        List<ByteBuffer> variables = new ArrayList<>();
        variables.add(ByteBufferUtil.bytes(String.valueOf(sum)));
        return variables;
    }

}
