package com.thinkbig;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LandingZoneMapper extends Mapper<LongWritable, Text, JunkDataJoinKey, BytesPairWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(LandingZoneMapper.class);

    public void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (value.getLength() < 5) {
            LOG.info("record is less than 5 - size = " + value.getLength());
            return;
        }
        Text venusQBinary = value;
        JunkData junkData = getPrimaryKeyEntries(venusQBinary);

        if (junkData.isValid() && venusQBinary.getLength() > 1) {

            JunkDataJoinKey junkDataJoinKey =
                    new JunkDataJoinKey(junkData, new IntWritable(1));
            context.write(junkDataJoinKey, new BytesPairWritable(key,
                    new Text(venusQBinary)));
        }
    }

    protected JunkData getPrimaryKeyEntries(Text venusQBinary) throws IOException {

        List<String> items = Arrays.asList(venusQBinary.toString().split("\\s*,\\s*"));

        System.out.println("STUFF: ");
        System.out.println("0: " + items.get(0));
        System.out.println("1:" + items.get(1));
        System.out.println("2: " + items.get(2));
        System.out.println("3: " + items.get(3));
        System.out.println("4: " + items.get(4));
        return new JunkData(items.get(0), items.get(1), items.get(4), items.get(2), items.get(3));
    }
}