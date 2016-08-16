package com.thinkbig;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rs186089 on 8/2/16.
 */
public class DedupReducer1 extends Reducer<JunkDataJoinKey, BytesPairWritable,
        Text, BytesPairWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(DedupReducer1.class);


    private MultipleOutputs mos;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
    }

    //The second parameter values, only the first one is used, even though there may
    //be more the one, this is consistent with Matt's explanation, that only one entry is used.
    //CHANGE THIS TO reduce on the partition key.
    public void reduce(JunkDataJoinKey key, Iterable<BytesPairWritable> values,
                       Context context) throws IOException, InterruptedException {

        Iterator<BytesPairWritable> valuesIterator = values.iterator();
        BytesPairWritable bytesPairWritable = valuesIterator.next();
        int count = Lists.newArrayList(bytesPairWritable).size();
        System.out.println("How Many: " + count);

        if (key.getSortOrder().equals(new IntWritable(0))) {
            //Entry already exists venusq_history table, do nothing.
            LOG.info("reducer: entry already exists!");
            return;
        }

        //Need to figure out in here how to collect each reduced value and aggregate them as one file, then write it.
        //Assumptions, a reducer reducers all it's key/value lists?

        String batchId = "abc123";
        String historicalPath = "history";
        String vqMasterBasePath = "vqm";

        try {
            if (key.getJunkData().isValid()) {
                //writeToVenusQMaster(vqMasterBasePath, key.getJunkData().getEndDate(), key.getJunkData().getProduct(),
                //        key.getJunkData().getProcId(), batchId, bytesPairWritable);

                writeHistoricalFile(key.getKey(), key.getJunkData().getEndDate(), batchId, historicalPath);

                String fullVQMasterPath = getMasterPartitionedPath(vqMasterBasePath, key.getJunkData().getProcId(),
                        key.getJunkData().getEndDate(), key.getJunkData().getProduct(), batchId, "text");


                context.write(new Text(fullVQMasterPath), bytesPairWritable);

            } else {
                LOG.error("Received an invalid venusq history record in the reducer: " + key.getJunkData().toString());
            }
        } catch (NumberFormatException nfe) {
            LOG.error("We had a bad date in the path and could not write the file: " + key.getJunkData().getEndDate());
        }
    }

    protected void writeToVenusQMaster(String vqMasterBasePath, String endDate,
                                       String product, String procId, String batchId,
                                       BytesPairWritable bytesPairWritable) throws IOException, InterruptedException {

        String fullVQMasterPath = getMasterPartitionedPath(vqMasterBasePath, procId, endDate, product, batchId, "text");

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

    protected void writeHistoricalFile(Text key, String endDate, String batchId,
                                       String historicalPath) throws IOException, InterruptedException {
        String year = endDate.substring(0, 4);
        System.out.println("PROGRESS: " +endDate);
        int month = Integer.parseInt(endDate.substring(4, 6));
        String historyPath = historicalPath + "/year=" + year + "/month="
                + month + "/batch_id=" + batchId + "/" + "history";

        mos.write("history", NullWritable.get(),
                key, historyPath);
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
