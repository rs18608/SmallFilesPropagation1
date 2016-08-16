package com.thinkbig;


import com.google.common.collect.Lists;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class MasterGoldenReducer extends Reducer<JunkDataJoinKey, BytesPairWritable,
        BytesWritable, BytesPairWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(DedupReducer1.class);

    private MultipleOutputs mos;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    public void reduce(JunkDataJoinKey key, Iterable<BytesPairWritable> values,
                       Context context) throws IOException, InterruptedException {

        Iterator<BytesPairWritable> valuesIterator = values.iterator();
        BytesPairWritable bytesPairWritable = valuesIterator.next();
        int count = Lists.newArrayList(bytesPairWritable).size();
        System.out.println("How Many: " + count);

        String batchId = "abc123";
        String vqMasterBasePath = "vqm";

        System.out.println("MASTERGOLDEN_REDUCER: " + key);
        System.out.println("MASTERGOLDER_REDUCER: " + values.toString());

        mos.write("master",bytesPairWritable.getKey(), bytesPairWritable.getVal(),key.toString());

    }

    protected void writeToVenusQMaster(String vqMasterBasePath, String endDate,
                                       String product, String procId, String batchId,
                                       BytesPairWritable bytesPairWritable) throws IOException, InterruptedException {

        String fullVQMasterPath = getMasterPartitionedPath(vqMasterBasePath,
                procId, endDate, product, batchId, "text");
//What VenusQHistoryJoinKey, this writes to a sequence file with the file type (seq), the key, value, and the path.
//Does mos.write write lists?

        //mos.write("/user/lynnscott/sf/master",bytesPairWritable.getKey(), bytesPairWritable.getVal(),fullVQMasterPath);
        System.out.println("HERE 1: " + bytesPairWritable.getKey().toString() );
        System.out.println("HERE 2: " + bytesPairWritable.getVal().toString() );
        System.out.println("HERE 3: " + fullVQMasterPath );

        mos.write("master",bytesPairWritable.getKey(), bytesPairWritable.getVal(),fullVQMasterPath);

    }


    protected String getMasterPartitionedPath(String tableBasePath,
                                              String procId, String endDate, String product, String batchId, String fileName) {
        String processGroup = procId;
        String year = endDate.substring(0, 4);
        int month = Integer.parseInt(endDate.substring(4, 6));

        return tableBasePath + "/product=" + product + "/process_group="
                + processGroup + "/year=" + year + "/month=" + month
                + "/batch_id=" + batchId + "/" + fileName;
    }


    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);

        int closeRetries = 3;
        boolean closeSuccessful = false;

        while (!closeSuccessful && closeRetries > 0) {
            try {
                mos.close();
                closeSuccessful = true;
            } catch (IOException e) {
                LOG.error(e.toString());
                LOG.error("Attempt to close file failed, " + closeRetries + " attempts remaining");
            }
            closeRetries--;
        }

        if (!closeSuccessful) {
            throw new IOException("Exceeded number of retries to close output file, failing file close");
        }
    }
}
