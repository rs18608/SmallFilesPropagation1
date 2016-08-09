package com.thinkbig;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

/**
 * Created by rs186089 on 8/1/16.
 */
public class JunkDataJoinKey implements WritableComparable<JunkDataJoinKey> {
    private Text key;
    private JunkData junkData;
    private IntWritable sortOrder;

    public JunkDataJoinKey() {
        this.key = new Text();
        this.sortOrder = new IntWritable();
        this.junkData = null;
    }

    public Text getKey() {
        return this.key;
    }

    public IntWritable getSortOrder() {
        return this.sortOrder;
    }

    public JunkData getJunkData() {
        return this.junkData;
    }

    /**
     * Constructs a join key instance from the given venusq history object
     *
     * @param venusQHistory A valid venusq history object
     * @param sortOrder     The order in which this entry should be sorted
     */
    public JunkDataJoinKey(final JunkData venusQHistory, final IntWritable sortOrder) {
        System.out.println("IN JunkDataJoinKey history: " + venusQHistory.toString());
        System.out.println("IN JunkDataJoinKey sortOrder: " + sortOrder.toString());


        System.out.println("IN JunkDatJoinKey venusqhistory: "+ venusQHistory.getEndDate() + " "+
                venusQHistory.getEventCode()+" "+
                venusQHistory.getProcId()+" "+
                venusQHistory.getProduct()+" "+
                venusQHistory.getSerial());
        String foo = venusQHistory.getEndDate() + ","+
                venusQHistory.getEventCode()+","+
                venusQHistory.getProcId()+","+
                venusQHistory.getProduct()+","+
                venusQHistory.getSerial();


        this.key = new Text(foo);
        //this.key = new Text(venusQHistory.toString());

        System.out.println("ZERO: " + this.key.toString());

        this.sortOrder = sortOrder;
        // Create a defensive copy
        this.junkData = new JunkData(venusQHistory);

        System.out.println("IN JunkDatJoinKey returning: "+ junkData.getEndDate() + " "+
                junkData.getEventCode()+" "+
                junkData.getProcId()+" "+
                junkData.getProduct()+" "+
                junkData.getSerial());
    }

    //@Override
    public int compareTo(JunkDataJoinKey junkDataJoinKey) {
        int keyCompare = this.key.compareTo(junkDataJoinKey.getKey());
        if (keyCompare == 0) {
            keyCompare = this.sortOrder.compareTo(junkDataJoinKey.getSortOrder());
        }

        return keyCompare;
    }

    //@Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        sortOrder.write(out);
    }

    //@Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        sortOrder.readFields(in);
        // Create a venusq object based on the read fields
        try {
            System.out.println("DATA INPUT: " + this.key.toString());
            System.out.println("ONE: " + this.key.toString());
            this.junkData = new JunkData(this.key.toString());
        } catch (ParseException pe) {
            // We are swallowing this for now
        }
    }

    @Override
    public int hashCode() {
        int result = junkData != null ? junkData.hashCode() : 0;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JunkDataJoinKey) {
            JunkDataJoinKey junkDataJoinKey = (JunkDataJoinKey) o;
            return key.equals(junkDataJoinKey.getKey())
                    && sortOrder.equals(junkDataJoinKey.getSortOrder());
        }
        return false;
    }

    @Override
    public String toString() {
        return key.toString() + "," + sortOrder.toString();
    }
}