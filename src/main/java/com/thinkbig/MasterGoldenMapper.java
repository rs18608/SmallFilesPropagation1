package com.thinkbig;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MasterGoldenMapper extends Mapper<Text, BytesPairWritable, Text, BytesPairWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(LandingZoneMapper.class);

    public void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    public void map(Text key, BytesPairWritable value, Context context)
            throws IOException, InterruptedException {

        System.out.println("MASTERGOLDERMAPPER: " + key);
        System.out.println("MASTERGOLDERMAPPER: " + value.toString());

            context.write(key, value);
    }

}
