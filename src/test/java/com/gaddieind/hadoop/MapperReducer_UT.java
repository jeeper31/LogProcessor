package com.gaddieind.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by agaddie on 10/3/2015.
 */
public class MapperReducer_UT {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        ExceptionMapper mapper = new ExceptionMapper();
        ExceptionFileSystemReducer reducer = new ExceptionFileSystemReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(),
                new Text("2015-09-24 09:26:15,576 WARN  http-nio-80-exec-33 [org.apache.cxf.jaxrs.impl.WebApplicationExceptionMapper] javax.ws.rs.NotFoundException: Could not find a region with the id: 42233527-4d32-4e8f-9fb8-8c29889b9113\tat com.rfcontrols.rm.dataservices.services.region.RegionResource.failIfDoesNotExist(RegionResource.java:282)\tat com.rfcontrols.rm.dataservices.services.region.RegionResource.getRegion(RegionResource.java:80)\tat sun.reflect.GeneratedMethodAccessor143.invoke(Unknown Source)\t"));
        mapDriver.withOutput(new Text("RegionResource.java:282"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.withInput(new LongWritable(),
                new Text("2015-09-24 09:26:15,576 WARN  http-nio-80-exec-33 [org.apache.cxf.jaxrs.impl.WebApplicationExceptionMapper] javax.ws.rs.NotFoundException: Could not find a region with the id: 42233527-4d32-4e8f-9fb8-8c29889b9113\tat com.rfcontrols.rm.dataservices.services.region.RegionResource.failIfDoesNotExist(RegionResource.java:282)\tat com.rfcontrols.rm.dataservices.services.region.RegionResource.getRegion(RegionResource.java:80)\tat sun.reflect.GeneratedMethodAccessor143.invoke(Unknown Source)\t"));
        List<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1));
        mapReduceDriver.withOutput(new Text("RegionResource.java:282"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}
