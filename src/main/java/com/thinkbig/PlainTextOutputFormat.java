package com.thinkbig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PlainTextOutputFormat<K, V> extends TextOutputFormat<K, V> {
    public PlainTextOutputFormat() { }

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        String keyValueSeparator = conf.get("mapred.textoutputformat.separator", "\t");
        String extension = "";

        Path file1 = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file1.getFileSystem(conf);
        FSDataOutputStream fileOut;
        fileOut = fs.create(file1, false);

        return new LineRecordWriter(fileOut, keyValueSeparator);
    }
}