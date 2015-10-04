package com.gaddieind.hadoop;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by agaddie on 10/3/2015.
 */
public class ExceptionMapper_UT {

  @Test
  public void testPattern(){
    String val = "2015-09-24 09:26:00,757 WARN  http-nio-80-exec-145 [org.apache.cxf.jaxrs.impl.WebApplicationExceptionMapper] javax.ws.rs.NotFoundException: Could not find a region with the id: 15ba5703-d80e-4f18-bbd7-5a5848f82026";

   String regex = "(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2},\\d{3})";
   Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(val);

   assertTrue(m.find());
  }

    @Test
    public void splitOnStacktraces() throws Exception{
        Configuration conf = new Configuration(false);
        //conf.set("fs.default.name", "hdfs://file/");
        System.setProperty("hadoop.home.dir", "C:\\java-tools\\hadoop");

        File testFile = new File("src/test/resources/dataServices.log");
        Path path = new Path(testFile.getAbsoluteFile().toURI().toString());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        InputFormat inputFormat = ReflectionUtils.newInstance(RegxInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContext(conf, new TaskAttemptID());
        RecordReader reader = inputFormat.createRecordReader(split, context);

        reader.initialize(split, context);
        reader.nextKeyValue();
        reader.nextKeyValue();
        Text val = (Text) reader.getCurrentValue();
        assertNotNull(val);

        System.out.println("Value---");
        System.out.println(new String(val.getBytes()));
    }
}
