package com.thinkbig;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    public GroupingComparator() {
        super(JunkDataJoinKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JunkDataJoinKey key1 = (JunkDataJoinKey) a;
        JunkDataJoinKey key2 = (JunkDataJoinKey) b;
        return key1.getKey().compareTo(key2.getKey());
    }
}