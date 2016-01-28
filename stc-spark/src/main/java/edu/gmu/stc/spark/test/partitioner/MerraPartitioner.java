package edu.gmu.stc.spark.test.partitioner;

import org.apache.spark.Partitioner;

/**
 * Created by feihu on 1/14/16.
 */
public class MerraPartitioner extends Partitioner {
    @Override
    public int numPartitions() {
        return 0;
    }

    @Override
    public int getPartition(Object key) {
        return 0;
    }
}
