package com.thinkbig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class DedupPrototype extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);

        Path path1 = new Path("/user/lynnscott/sf/history");
        Path path2 = new Path("/user/lynnscott/sf/landingzone");


        Path outputPath = new Path("/home/lynnscott/sf/master");
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        // It will delete the output directory if it already exists. don't need
        // to delete it manually
        fs.delete(outputPath, true);

        MultipleInputs.addInputPath(job, path1, CombineTextInputFormat.class, VenusQHistoryMapper.class);
        MultipleInputs.addInputPath(job, path2, CombineTextInputFormat.class, LandingZoneMapper.class);


        job.setReducerClass(DedupReducer1.class);
        job.setMapOutputKeyClass(JunkDataJoinKey.class);
        job.setMapOutputValueClass(BytesPairWritable.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setSortComparatorClass(SortingComparator.class);
        job.setJarByClass(VenusQHistoryMapper.class);

        Path output = new Path("/user/lynnscott/sf/master");
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete(output, true);

        FileOutputFormat.setOutputPath(job, new Path("/user/lynnscott/sf/master"));

        MultipleOutputs.addNamedOutput(job, "history",
                PlainTextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "master",
                PlainTextOutputFormat.class, Text.class, Text.class);

/*

        job.setJobName("DedupPrototype");
        job.setJarByClass(DedupPrototype.class);
        Path outputPath = new Path("/home/lynnscott/mapper/output");
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        // It will delete the output directory if it already exists. don't need
        // to delete it manually
        fs.delete(outputPath, true);

        // Setting the input and output path
        FileInputFormat.setInputPaths(job, "/user/lynnscott/data");
        FileOutputFormat.setOutputPath(job, outputPath);

        // Considering the input and output as text file set the input & output
        // format to TextInputFormat
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        ChainMapper.addMapper(job, TokenizerMapper.class, LongWritable.class,
                Text.class, Text.class, IntWritable.class, new Configuration(
                        false));

        // addMapper will take global conf object and mapper class ,input and
        // output type for this mapper and output key/value have to be sent by
        // value or by reference and localJObconf specific to this call

        ChainMapper.addMapper(job, UpperCaserMapper.class, Text.class,
                IntWritable.class, Text.class, IntWritable.class,
                new Configuration(false));

        ChainReducer.setReducer(job, WordCountReducer.class, Text.class,
                IntWritable.class, Text.class, IntWritable.class,
                new Configuration(false));

        ChainReducer.addMapper(job, LastMapper.class, Text.class,
                IntWritable.class, Text.class, IntWritable.class,
                new Configuration(false));*/
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new DedupPrototype(), args);
        System.exit(res);
    }
}