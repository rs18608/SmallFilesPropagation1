package com.thinkbig;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingComparator extends WritableComparator {
    public SortingComparator() {
        super(JunkDataJoinKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JunkDataJoinKey key1 = (JunkDataJoinKey) a;
        JunkDataJoinKey key2 = (JunkDataJoinKey) b;
        int keyComparator = key1.getKey().compareTo(key2.getKey());
        if (keyComparator == 0) {
            return key1.getSortOrder().compareTo(key2.getSortOrder());
        }
        return keyComparator;
    }
}