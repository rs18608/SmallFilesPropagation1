package com.thinkbig;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

public class VenusQHistoryMapper extends Mapper<LongWritable, Text, JunkDataJoinKey, BytesPairWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(VenusQHistoryMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Try and convert this value into a venusq history object, and ignore if it is not a valid value, but
        // log the error
        try {

            JunkData junkData = new JunkData(value.toString());
            JunkDataJoinKey junkDataJoinKey = new JunkDataJoinKey(junkData, new IntWritable(0));
            context.write(junkDataJoinKey,
                    new BytesPairWritable(new LongWritable(), new Text("history")));

        } catch (ParseException pe) {
            LOG.error("Failed to successfully parse the entry: '" + value.toString() + "'");
            LOG.error(pe.getMessage());
        }
    }
}
