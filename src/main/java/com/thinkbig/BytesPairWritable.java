package com.thinkbig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BytesPairWritable implements Writable {

    private LongWritable key;
    private Text val;

    public BytesPairWritable(LongWritable key, Text val) {
        this.key = key;
        this.val = val;
    }

    public BytesPairWritable() {
        this(new LongWritable(), new Text());
    }

    public LongWritable getKey() {
        return key;
    }

    public Text getVal() {
        return val;
    }

    //@Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        val.readFields(in);
    }

    //@Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        val.write(out);
    }
}